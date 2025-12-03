.PHONY: all build clean srpm tarball test versions

OUTPUT=dnstapir-edm
SPECFILE_IN:=rpm/dnstapir-edm.spec.in
SPECFILE_OUT:=rpm/SPECS/dnstapir-edm.spec
DEB_CONTROL_IN:=deb/control.in
DEB_CONTROL_OUT:=deb/DEBIAN/control

all:

test:
	GOOS= GOARCH= go test -race ./...

build: export GOSUMDB=sum.golang.org
build: export GOTOOLCHAIN=auto
build:
	CGO_ENABLED=0 go build -ldflags "-X main.version=$(shell test -f VERSION && cat VERSION || echo dev)" github.com/dnstapir/edm/cmd/dnstapir-edm

clean: SHELL:=/bin/bash
clean:
	-rm -f $(OUTPUT)
	-rm -f VERSION
	-rm -f RPM_VERSION
	-rm -f DEB_VERSION
	-rm -f *.tar.gz
	-rm -f rpm/SOURCES/*.tar.gz
	-rm -rf rpm/{BUILD,BUILDROOT,SPECS,SRPMS,RPMS}
	@rm -rf deb/usr
	@rm -rf deb/etc
	@rm -rf deb/var
	@rm -rf deb/DEBIAN/control
	@rm -rf *.deb
	@rm -rf out

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

rpm: srpm
	mkdir ./out
	cp -r ./rpm ./out/
	rpmbuild --rebuild --define "%_topdir $$(pwd)/out/rpm" --undefine=dist $$(pwd)/out/rpm/SRPMS/$(OUTPUT)-$$(cat RPM_VERSION)-*.src.rpm
	test -z "$(outdir)" || cp $$(pwd)/out/rpm/RPMS/*/$(OUTPUT)-$$(cat RPM_VERSION)-*.rpm "$(outdir)"

deb: build versions
	mkdir -p deb/usr/bin
	mkdir -p deb/etc/dnstapir/edm
	mkdir -p deb/var/lib/dnstapir/edm/pebble
	mkdir -p deb/var/lib/dnstapir/edm/mqtt
	mkdir -p deb/usr/lib/systemd/system
	cp dnstapir-edm deb/usr/bin
	cp rpm/SOURCES/dnstapir-edm.service deb/usr/lib/systemd/system
	cp rpm/SOURCES/well-known-domains.dawg deb/etc/dnstapir/edm
	cp rpm/SOURCES/ignored.dawg deb/etc/dnstapir/edm
	cp rpm/SOURCES/ignored-ips deb/etc/dnstapir/edm
	sed -e "s/@@VERSION@@/$$(cat DEB_VERSION)/g" $(DEB_CONTROL_IN) > $(DEB_CONTROL_OUT)
	dpkg-deb -b deb dnstapir-edm-$$(cat DEB_VERSION).deb
