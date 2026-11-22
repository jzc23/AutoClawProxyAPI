# 构建阶段：纯 Go 零第三方依赖，静态编译无需 CGO
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /out/autoclaw2api ./cmd/autoclaw2api

# 运行阶段：alpine + CA 证书 + 时区数据，非 root 运行
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -H -u 10001 autoclaw
COPY --from=build /out/autoclaw2api /usr/local/bin/autoclaw2api
WORKDIR /app
USER autoclaw
EXPOSE 8080
VOLUME ["/app/auths", "/app/data"]
ENTRYPOINT ["autoclaw2api"]
CMD ["-config", "/app/config.json"]
