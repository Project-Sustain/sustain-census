# --------------------------------------------------------------------
# Author: Menuka Warushavithana
# --------------------------------------------------------------------

.PHONY: build
build:
	chmod +x gradlew
	./gradlew installDist -x test

build-with-tests:
	chmod +x gradlew
	./gradlew install

run-sustain-server:
	export DB_NAME="sustaindb"
	export DB_USERNAME=""
	export DB_PASSWORD=""
	export DB_HOST="lattice-167"
	export DB_PORT=27017
	./gradlew install -x test
	sh ./build/install/sustain-census-grpc/bin/sustain-server

test:
	./gradlew test

run-spatial-client:
	export DB_NAME="sustaindb"
	export DB_HOST="lattice-167"
	export DB_PORT=27017
	export SERVER_HOST="lattice-167"
	sh ./build/install/sustain-census-grpc/bin/spatial-client

run-spark-job:
	sh ./build/install/sustain-census-grpc/bin/spark-job

proto:
	./gradlew generateProto

clean:
	rm -rf build
