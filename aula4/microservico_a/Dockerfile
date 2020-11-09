FROM golang:1.15

WORKDIR /go/src/microservico_a

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN GOOS=linux go build

EXPOSE 9090

ENTRYPOINT ["./microservico_a"]