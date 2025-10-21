ARCH=		$(shell arch)
TEST_ARCH=
OUTPUT=dnstapir-edm
SPECFILE_IN:=rpm/dnstapir-edm.spec.in
SPECFILE_OUT:=rpm/SPECS/dnstapir-edm.spec

run_tests=	yes
ifdef TEST_ARCH
ifneq "$(TEST_ARCH)" "$(ARCH)"
run_tests=	no
endif
endif


all:

container:
	KO_DOCKER_REPO=ko.local ko build --bare

build: export GOSUMDB=sum.golang.org
build: export GOTOOLCHAIN=auto
build:
	go mod download
ifeq "$(run_tests)" "yes"
	go vet ./...
	go test -race ./...
endif
	CGO_ENABLED=0 go build -ldflags "-X main.version=$(shell test -f VERSION && cat VERSION || echo dev)" github.com/dnstapir/edm/cmd/dnstapir-edm

clean: SHELL:=/bin/bash
clean:
	-rm -f $(OUTPUT)
	-rm -f VERSION
	-rm -f RPM_VERSION
	-rm -f *.tar.gz
	-rm -f rpm/SOURCES/*.tar.gz
	-rm -rf rpm/{BUILD,BUILDROOT,SPECS,SRPMS,RPMS}

versions:
	./gen-versions.sh

tarball: versions
	git archive --format=tar.gz --prefix=$(OUTPUT)/ -o $(OUTPUT).tar.gz --add-file VERSION HEAD

srpm: SHELL:=/bin/bash
srpm: tarball
	mkdir -p rpm/{BUILD,RPMS,SRPMS,SPECS}
	sed -e "s/@@VERSION@@/$$(cat RPM_VERSION)/g" $(SPECFILE_IN) > $(SPECFILE_OUT)
	cp $(OUTPUT).tar.gz rpm/SOURCES/
	rpmbuild -bs --define "%_topdir ./rpm" --undefine=dist $(SPECFILE_OUT)
	test -z "$(outdir)" || cp rpm/SRPMS/*.src.rpm "$(outdir)"
