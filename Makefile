# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.EXPORT_ALL_VARIABLES:

SERVER_HOST = "lattice-167"
DB_NAME = "sustaindb"
DB_USERNAME = ""
DB_PASSWORD = ""
DB_HOST = "lattice-46"
DB_PORT = 27017

.PHONY: build
build:
	chmod +x gradlew
	./gradlew installDist
	./gradlew installShadowDist

build-with-tests:
	chmod +x gradlew
	./gradlew install

run-sustain-server:
	./gradlew installDist
	./build/install/sustain-census-grpc/bin/sustain-server

test:
	./gradlew test

run-spatial-client:
	./build/install/sustain-census-grpc/bin/spatial-client

run-spark-job:
	./build/install/sustain-census-grpc/bin/spark-job

proto:
	./gradlew generateProto

clean:
	rm -rf build
