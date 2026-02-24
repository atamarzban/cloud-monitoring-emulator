FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /emulator ./cmd/emulator

FROM scratch
COPY --from=builder /emulator /emulator
EXPOSE 8080
ENTRYPOINT ["/emulator"]
