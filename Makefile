PROG:=tapir-edm
ARCH=		$(shell arch)
TEST_ARCH=
OUTPUT=		edm
VERSION:=`cat ./VERSION`

run_tests=	yes
ifdef TEST_ARCH
ifneq "$(TEST_ARCH)" "$(ARCH)"
run_tests=	no
endif
endif


all:

container:
	KO_DOCKER_REPO=ko.local ko build --bare

build:
	go mod download
ifeq "$(run_tests)" "yes"
	go vet ./...
	go test -race ./...
endif
	CGO_ENABLED=0 go build -o $(OUTPUT)

clean:
	-rm -f $(OUTPUT)
	-rm -rf dist/rpm/SPECS/*spec dist/rpm/RPMS dist/rpm/BUILD dist/rpm/SOURCES/$(PROG) dist/rpm/SRPMS dist/rpm/BUILDROOT
	-rm -rf dist/src/
	-rm -rf dist/bin/

srcdist:
	-mkdir -p dist/src
	git archive --format=tar.gz --prefix=$(PROG)/ -o dist/src/$(PROG).tar.gz HEAD

bindist: srcdist
	-mkdir -p dist/bin/build
	cp dist/src/$(PROG).tar.gz dist/bin/build/
	tar xvf dist/bin/build/$(PROG).tar.gz -C dist/bin/build
	rm -f dist/bin/build/*.tar.gz
	cd dist/bin/build/$(PROG) && go build -o $(PROG) -ldflags="-X main.version=$(git log -1 --pretty=%H) -B gobuildid"
	mv dist/bin/build/$(PROG)/$(PROG) dist/bin/

rpm: bindist
	-mkdir -p dist/rpm/SPECS dist/rpm/RPMS dist/rpm/BUILD dist/rpm/SOURCES dist/rpm/SRPMS
	cp dist/bin/$(PROG) dist/rpm/SOURCES
	sed -e "s/@@VERSION@@/$(VERSION)/g" dist/rpm/SPECS/$(PROG).spec.in > dist/rpm/SPECS/$(PROG).spec
	cd dist/rpm && rpmbuild --define "_topdir `pwd`" -v -ba SPECS/$(PROG).spec


