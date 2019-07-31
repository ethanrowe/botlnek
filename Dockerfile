FROM golang:1.12.5 AS source
COPY . /botlnek
WORKDIR /botlnek
RUN make clean

FROM source AS deps
RUN go mod download

FROM deps AS build
RUN make build

FROM golang:1.12.5 AS run
COPY --from=build /botlnek/bin /botlnek
WORKDIR /
EXPOSE 8080
ENTRYPOINT ["/botlnek/restserver"]

