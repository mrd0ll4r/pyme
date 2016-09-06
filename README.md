# pyme

[![Build Status](https://api.travis-ci.org/mrd0ll4r/pyme.svg?branch=master)](https://travis-ci.org/mrd0ll4r/pyme)
[![Go Report Card](https://goreportcard.com/badge/github.com/mrd0ll4r/pyme)](https://goreportcard.com/report/github.com/mrd0ll4r/pyme)
[![GoDoc](https://godoc.org/github.com/mrd0ll4r/pyme?status.svg)](https://godoc.org/github.com/mrd0ll4r/pyme)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Task distribution for the [Python Microscopy Environment].

[Python Microscopy Environment]: https://bitbucket.org/david_baddeley/python-microscopy

## Installation

We use [glide] for dependency management.
To install the two binaries produces by this repository:

1. You must have a working go 1.7+ installation:
    Get Go from the [Download Go] page.
    Install Go as per the [Installation Instructions].
    Set your `$GOPATH`, add `$GOPATH/bin` and the Go binaries to your `$PATH`.

2. You must have glide installed:

    ```
    curl https://glide.sh/get | sh
    ```
    Or get a binary from the [glide releases] page.

3. Fetch this repository:

    ```
    go get github.com/mrd0ll4r/pyme
    ```
    This will fetch the repository to `$GOPATH/src/github.com/mrd0ll4r/pyme`.

4. Navigate to the directory and execute

    ```
    glide install
    ```
    This will create and populate the `vendor/` directory with dependencies.

5. Install the binaries:

    ```
    go install ./cmd/distributor
    go install ./cmd/nodeserver
    ```

The binaries will be installed to your `$GOPATH/bin`.

[glide]: https://github.com/Masterminds/glide
[glide releases]: https://github.com/Masterminds/glide/releases
[Download Go]: https://golang.org/dl/
[Installation Instructions]: https://golang.org/doc/install



