FROM golang:1.26.1-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY main.go templates.go ./
COPY templates ./templates
RUN go build -o /waiservability .

FROM alpine:3.22
WORKDIR /app
COPY --from=builder /waiservability /app/waiservability
EXPOSE 7070
ENTRYPOINT ["/app/waiservability"]
