package main

import "errors"

var (
	// ErrLoadRouteFile fail to open the routing file
	ErrLoadRouteFile = errors.New("failed to load route file")
	// ErrRouteFormat the format of the route file is incorrect
	ErrRouteFormat = errors.New("wrong route format")
)
