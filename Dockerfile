FROM golang:1.15.6-alpine as builder

LABEL maintainer="Oleg Ozimok ozimokoleg@gmail.com"

COPY . /go/src/github.com/kismia/copysql

WORKDIR /go/src/github.com/kismia/copysql/cmd/copysql

RUN go build -o /copysql .

FROM alpine:3.7

COPY --from=builder /copysql /usr/bin/copysql

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/bin/copysql"]