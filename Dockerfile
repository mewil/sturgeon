FROM golang:1.16-alpine AS build
ENV CGO_ENABLED=0
COPY . /go/src/github.com/mewil/sturgeon
WORKDIR /go/src/github.com/mewil/sturgeon
RUN go mod download
RUN go install .
RUN ls /go/bin
RUN adduser -D -g '' user

FROM scratch AS sturgeon
LABEL Author="Michael Wilson"
COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /go/bin/sturgeon /bin/sturgeon
USER user
ENTRYPOINT ["/bin/sturgeon"]
EXPOSE 8080