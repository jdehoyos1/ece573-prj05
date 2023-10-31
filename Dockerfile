# Stage 1: Compile the binary in a containerized Golang environment
#
FROM golang:1.21 as build

# Copy the source files from the host
COPY . /go/src

WORKDIR /go/src/clients
RUN CGO_ENABLED=0 GOOS=linux go build -o clients

# Stage 2: Build the image for client
#
FROM scratch as image

# Copy the binary from the build container
COPY --from=build /go/src/clients/clients .

# Tell Docker to execute this command on a "docker run"
CMD ["/clients"]
