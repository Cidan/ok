# The ok image: the one ok binary, static, on distroless — every server role (ok serve, ok <role>
# serve) and the admin commands. Build from the repository's root:
#
#   docker build -f deploy/images/ok.Dockerfile -t ok .
FROM golang:1.26.9-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/ok ./cmd/ok

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/ok /usr/local/bin/ok
USER nonroot
EXPOSE 8472 8473
ENTRYPOINT ["/usr/local/bin/ok"]
