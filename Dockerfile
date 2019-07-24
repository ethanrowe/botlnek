FROM golang:1.12.5 AS build

COPY . /botlnek
WORKDIR /botlnek
RUN make clean
RUN make build

FROM golang:1.12.5 AS run
COPY --from=build /botlnek/bin /botlnek
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/botlnek/restserver"]

