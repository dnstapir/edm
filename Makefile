ARCH=		$(shell arch)
TEST_ARCH=
OUTPUT=tapir-edm
SPECFILE_IN:=rpm/tapir-edm.spec.in
SPECFILE_OUT:=rpm/SPECS/tapir-edm.spec

VERSION:=$$(git describe --tags --abbrev=0 2> /dev/null || echo "v0.0.0")
SHA:=$$(git describe --dirty=+WiP --always)
DATE:=$$(date +%Y%m%d)

run_tests=	yes
ifdef TEST_ARCH
ifneq "$(TEST_ARCH)" "$(ARCH)"
run_tests=	no
endif
endif


all:

container:
	KO_DOCKER_REPO=ko.local ko build --bare

build: export GOTOOLCHAIN=auto
build:
	go mod download
ifeq "$(run_tests)" "yes"
	go vet ./...
	go test -race ./...
endif
	CGO_ENABLED=0 go build -o $(OUTPUT)

clean: SHELL:=/bin/bash
clean:
	-rm -f $(OUTPUT)
	-rm -f edm
	-rm -f VERSION
	-rm -f *.tar.gz
	-rm -f rpm/SOURCES/*.tar.gz
	-rm -rf rpm/{BUILD,BUILDROOT,SPECS,SRPMS,RPMS}

tarball:
	# Create VERSION file with version info and add it to tarball
	# Version string contains a snapshot as described here:
	#     https://docs.fedoraproject.org/en-US/packaging-guidelines/Versioning/#_snapshots
	echo "$(VERSION)^$(DATE).$(SHA)" > VERSION
	git archive --format=tar.gz --prefix=$(OUTPUT)/ -o $(OUTPUT).tar.gz --add-file VERSION HEAD

srpm: SHELL:=/bin/bash
srpm: tarball
	mkdir -p rpm/{BUILD,RPMS,SRPMS,SPECS}
	sed -e "s/@@VERSION@@/$$(cat VERSION)/g" $(SPECFILE_IN) > $(SPECFILE_OUT)
	cp $(OUTPUT).tar.gz rpm/SOURCES/
	rpmbuild -bs --define "%_topdir ./rpm" --undefine=dist $(SPECFILE_OUT)
	test -z "$(outdir)" || cp rpm/SRPMS/*.src.rpm "$(outdir)"
