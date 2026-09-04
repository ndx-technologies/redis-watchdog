package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type checkType int

const (
	checkSCard  checkType = iota // Redis SET — use SCARD
	checkExists                  // Redis string — use EXISTS
)

func (t checkType) String() string {
	switch t {
	case checkSCard:
		return "scard"
	case checkExists:
		return "exists"
	default:
		return fmt.Sprintf("checkType(%d)", int(t))
	}
}

func parseCheckType(s string) (checkType, error) {
	switch strings.ToLower(s) {
	case checkSCard.String():
		return checkSCard, nil
	case checkExists.String():
		return checkExists, nil
	default:
		return 0, fmt.Errorf("unknown check type %q (want %q or %q)", s, checkSCard, checkExists)
	}
}

type check struct {
	key     string    // Redis key to inspect
	cronJob string    // k8s CronJob name to trigger
	label   string    // human-readable label
	typ     checkType // how to check if key is populated
}

// checkFlag implements flag.Value for repeatable -check=key,cronjob,label,type flags.
type checkFlag []check

func (c *checkFlag) String() string {
	var sb strings.Builder
	for i, ch := range *c {
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(ch.key)
		sb.WriteByte(',')
		sb.WriteString(ch.cronJob)
		sb.WriteByte(',')
		sb.WriteString(ch.label)
		sb.WriteByte(',')
		sb.WriteString(ch.typ.String())
	}
	return sb.String()
}

func (c *checkFlag) Set(value string) error {
	parts := strings.Split(value, ",")
	if len(parts) != 4 {
		return fmt.Errorf("check %q must have form key,cronjob,label,type", value)
	}
	for i, name := range []string{"key", "cronjob", "label"} {
		if strings.TrimSpace(parts[i]) == "" {
			return fmt.Errorf("check %q: %s must not be empty", value, name)
		}
	}
	typ, err := parseCheckType(parts[3])
	if err != nil {
		return fmt.Errorf("check %q: %w", value, err)
	}
	*c = append(*c, check{
		key:     parts[0],
		cronJob: parts[1],
		label:   parts[2],
		typ:     typ,
	})
	return nil
}

const doc = `Keep connection to Redis and ping its health.
If Redis is restarted fresh new, it triggers kubectl jobs to warm it up.

Primary motivation for this is to avoid k8s CronJob and its oveheard and reduce pings to 5s.`

func main() {
	var (
		redisURL   string
		namespace  string
		dryRun     bool
		pingPeriod time.Duration
		checks     checkFlag
	)
	flag.StringVar(&redisURL, "redis", "redis://localhost:6379", "Redis URL")
	flag.StringVar(&namespace, "namespace", "default", "Kubernetes namespace")
	flag.BoolVar(&dryRun, "dry-run", true, "dry run: log actions but do not create k8s jobs")
	flag.DurationVar(&pingPeriod, "ping-period", 5*time.Second, "Redis ping interval")
	flag.Var(&checks, "check", "comma-separated check key,cronjob,label,type where type is scard or exists; repeatable; e.g. -check=\"bakery:orders:open:ids,bakery-order-populate-cron,open orders,scard\"")
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), doc)
		flag.PrintDefaults()
	}
	flag.Parse()

	if len(checks) == 0 {
		log.Fatal("checks are requred")
	}

	ctx := context.Background()

	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("invalid redis url: %s", err)
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("cannot get in-cluster k8s config: %s", err)
	}
	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		log.Fatalf("cannot create k8s client: %s", err)
	}

	slog.InfoContext(ctx, "redis-watchdog started", "checks", len(checks))

	triggered := make(map[string]bool)
	redisWasDown := false

	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-sig:
			return
		case <-ticker.C:
		}

		if err := rdb.Ping(ctx).Err(); err != nil {
			if !redisWasDown {
				slog.WarnContext(ctx, "redis unreachable", "error", err)
				redisWasDown = true
			}
			continue
		}

		if redisWasDown {
			slog.InfoContext(ctx, "redis is back online, checking data integrity")
			redisWasDown = false
			triggered = make(map[string]bool)
		}

		for _, c := range checks {
			if triggered[c.cronJob] {
				continue
			}

			empty, err := isRedisKeyEmpty(ctx, rdb, c)
			if err != nil {
				slog.ErrorContext(ctx, "failed to check key", "key", c.key, "error", err)
				continue
			}
			if !empty {
				continue
			}

			// Guard against duplicate watchdog-triggered jobs.
			if hasRunningWatchdogJob(ctx, clientset, namespace, c.cronJob) {
				slog.InfoContext(ctx, "watchdog job already running, skipping", "cronjob", c.cronJob)
				triggered[c.cronJob] = true
				continue
			}

			slog.WarnContext(ctx, "redis key is empty, triggering recovery", "key", c.key, "cronjob", c.cronJob, "label", c.label)
			if dryRun {
				slog.InfoContext(ctx, "DRY-RUN: would create job from cronjob", "cronjob", c.cronJob)
				triggered[c.cronJob] = true
				continue
			}

			if err := createJobFromCronJob(ctx, clientset, namespace, c.cronJob); err != nil {
				slog.ErrorContext(ctx, "failed to create recovery job", "cronjob", c.cronJob, "error", err)
				continue
			}

			slog.InfoContext(ctx, "recovery job created", "cronjob", c.cronJob, "label", c.label)
			triggered[c.cronJob] = true
		}
	}
}

func isRedisKeyEmpty(ctx context.Context, rdb *redis.Client, c check) (bool, error) {
	switch c.typ {
	case checkSCard:
		n, err := rdb.SCard(ctx, c.key).Result()
		if err != nil {
			return false, err
		}
		return n == 0, nil
	case checkExists:
		n, err := rdb.Exists(ctx, c.key).Result()
		if err != nil {
			return false, err
		}
		return n == 0, nil
	default:
		return false, fmt.Errorf("unknown check type: %v", c.typ)
	}
}

func hasRunningWatchdogJob(ctx context.Context, clientset *kubernetes.Clientset, namespace, cronJobName string) bool {
	jobs, err := clientset.BatchV1().Jobs(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return true
	}
	prefix := cronJobName + "-"
	for _, j := range jobs.Items {
		if j.Status.Active > 0 && strings.HasPrefix(j.Name, prefix) {
			return true
		}
	}
	return false
}

func createJobFromCronJob(ctx context.Context, clientset *kubernetes.Clientset, namespace, cronJobName string) error {
	cronJob, err := clientset.BatchV1().CronJobs(namespace).Get(ctx, cronJobName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-watchdog-%d", cronJobName, time.Now().Unix()),
			Namespace: namespace,
			Labels:    map[string]string{"triggered-by": "redis-watchdog"},
		},
		Spec: cronJob.Spec.JobTemplate.Spec,
	}

	if _, err := clientset.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return err
	}

	return nil
}
