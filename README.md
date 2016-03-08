Website-Upload-Processor
========================

The java service to manage uploads to the website.

First complete the config in "config/local/config.properties" (using the config.properties.sample file).

Then to build:

```
docker build -t website-upload-processor .
```
(or you can use the `build.sh` script)

To install and run (and automatically start whenever the docker service starts):
```
docker run -d \
  --name website-upload-processor \
  -v `pwd`/config:/usr/src/app/config \
  -v [PATH TO 'files' DIRECTORY ON FILE STORE HERE]:/usr/src/app/files \
  --restart=always \
  website-upload-processor

```

To stop:
```
docker stop website-upload-processor
```

To run:
```
docker start website-upload-processor
```

To view live output:
```
docker logs -f website-upload-processor
```

Simple!

Website repo at: https://github.com/LA1TV/Website
