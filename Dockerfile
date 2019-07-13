FROM golang:1.12.5 AS build

COPY . /go/src/github.com/ethanrowe/botlnek
WORKDIR /go/src/github.com/ethanrowe/botlnek
RUN make clean
RUN make build

FROM golang:1.12.5 AS run
COPY --from=build /go/src/github.com/ethanrowe/botlnek/bin /botlnek
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/botlnek/restserver"]

