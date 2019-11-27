FROM golang:latest as builder

# Copy the code from the host and compile it
WORKDIR /wongdim
ENV GO111MODULE=on
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags "-s -w" -installsuffix nocgo -o /wongdimbot ./cmd/tgbot \
 && CGO_ENABLED=0 GOOS=linux go build -a -ldflags "-s -w" -installsuffix nocgo -o /migrate_bleve ./cmd/bleve_migrate

FROM alpine:latest
COPY --from=builder /wongdimbot ./
COPY --from=builder /migrate_bleve ./
ENTRYPOINT ["./wongdimbot"]
EXPOSE 80/tcp