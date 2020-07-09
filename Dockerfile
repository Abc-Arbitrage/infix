FROM golang:1.14 as builder

WORKDIR /go/src/github.com/Abc-Arbitrage/infix

RUN go get -u github.com/golang/dep/cmd/dep

COPY Gopkg.lock .
COPY Gopkg.toml .

COPY . .

RUN dep ensure

ENV GOOS linux
ENV GOARCH amd64
ENV CGO_ENABLED=0

WORKDIR /go/src/github.com/Abc-Arbitrage/infix/command

RUN go build -v -o infix
