#/bin/sh

debuild -rfakeroot -uc -us -tc --lintian-opts --profile debian
#dpkg-buildpackage -rfakeroot -uc -us -tc

