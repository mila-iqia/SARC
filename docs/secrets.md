# Setup for the "secrets" folder

This is based on https://linuxconfig.org/using-openssl-to-encrypt-messages-and-files-on-linux

## First-time setup.
This was done once by Guillaume. You never need to do it again,
but you do need to have the file `sarc_global.key` provided
through some secure external channel.
```
openssl genrsa -aes256 -out sarc_global.key 4096
```

Encrypt it before committing to git (if updates were done).
```
tar cf secrets.tar.gz secrets
openssl enc -aes-256-cbc -kfile sarc_global.key -in secrets.tar.gz -out secrets.tar.gz.encrypted
rm secrets.tar.gz
```

Decrypt it when you check out the repo
for the first time or if updates were made upstream.
```
openssl enc -aes-256-cbc -kfile sarc_global.key -d -in secrets.tar.gz.encrypted | tar xz
```

## Note about one extra thing
We did not do this because `tar` complained about stuff. It seemed like a valid thing, though.
```
tar cz secrets | openssl enc -aes-256 -out secrets.tar.gz.encrypted
```