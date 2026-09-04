# redis-watchdog

Keep connection to Redis and ping its health.
If Redis is restarted fresh new, it triggers kubectl jobs to warm it up.

Primary motivation for this is to avoid k8s CronJob and its oveheard and reduce pings to 5s.

```bash
redis-watchdog \
  -redis redis://redis:6379 \
  -namespace default \
  -dry-run=false \
  -ping-period 5s \
  -check="bakery:orders:open:ids,bakery-order-populate-cron,open orders,scard" \
  -check="bakery:pastries:all:ids,bakery-pastry-populate-cron,pastry catalog,exists"
```
