FROM golang:1.27-alpine AS build
RUN apk --no-cache add ca-certificates

WORKDIR /src
COPY . .
RUN go build -o main .

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /src/main /main

ENTRYPOINT [ "/main" ]
