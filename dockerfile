# BUILD FROM MAVEN BASE IMAGE
# this will build the project
FROM maven:3.2-jdk-7-onbuild

RUN apt-get update

# BUILD FFMPEG
# https://trac.ffmpeg.org/wiki/CompilationGuide/Ubuntu

RUN cd $HOME && mkdir ffmpeg_sources ffmpeg_build ffmpeg_bin

RUN apt-get -y --force-yes install autoconf automake build-essential libass-dev libfreetype6-dev \
  libsdl1.2-dev libtheora-dev libtool libva-dev libvdpau-dev libvorbis-dev libxcb1-dev libxcb-shm0-dev \
  libxcb-xfixes0-dev pkg-config texinfo zlib1g-dev

RUN apt-get -y --force-yes install yasm

RUN apt-get -y --force-yes install libx264-dev

RUN cd $HOME/ffmpeg_sources && wget -O fdk-aac.tar.gz https://github.com/mstorsjo/fdk-aac/tarball/master && \
    tar xzvf fdk-aac.tar.gz && cd mstorsjo-fdk-aac* && autoreconf -fiv && \
    ./configure --prefix="$HOME/ffmpeg_build" --disable-shared && \
    make && make install && make distclean

RUN apt-get -y --force-yes install cmake mercurial && \
    cd $HOME/ffmpeg_sources && hg clone https://bitbucket.org/multicoreware/x265 && \
    cd x265/build/linux && \
    PATH="$HOME/ffmpeg_bin:$PATH" cmake -G "Unix Makefiles" -DCMAKE_INSTALL_PREFIX="$HOME/ffmpeg_build" -DENABLE_SHARED:bool=off ../../source && \
    make && make install

RUN apt-get -y --force-yes install libmp3lame-dev

RUN apt-get -y --force-yes install libopus-dev

RUN cd $HOME/ffmpeg_sources && wget http://storage.googleapis.com/downloads.webmproject.org/releases/webm/libvpx-1.5.0.tar.bz2 && \
    tar xjvf libvpx-1.5.0.tar.bz2 && cd libvpx-1.5.0 && \
    PATH="$HOME/ffmpeg_bin:$PATH" ./configure --prefix="$HOME/ffmpeg_build" --disable-examples --disable-unit-tests && \
    PATH="$HOME/ffmpeg_bin:$PATH" make && make install && make clean

RUN cd $HOME/ffmpeg_sources && wget http://ffmpeg.org/releases/ffmpeg-snapshot.tar.bz2 && \
    tar xjvf ffmpeg-snapshot.tar.bz2 && cd ffmpeg && \
    PATH="$HOME/ffmpeg_bin:$PATH" PKG_CONFIG_PATH="$HOME/ffmpeg_build/lib/pkgconfig" ./configure \
    --prefix="$HOME/ffmpeg_build" \
    --pkg-config-flags="--static" \
    --extra-cflags="-I$HOME/ffmpeg_build/include" \
    --extra-ldflags="-L$HOME/ffmpeg_build/lib" \
    --bindir="$HOME/ffmpeg_bin" \
    --enable-gpl \
    --enable-libass \
    --enable-libfdk-aac \
    --enable-libfreetype \
    --enable-libmp3lame \
    --enable-libopus \
    --enable-libtheora \
    --enable-libvorbis \
    --enable-libvpx \
    --enable-libx264 \
    --enable-libx265 \
    --enable-nonfree && \
    make install && \
    make distclean && \
    hash -r

# copy to bin
RUN cp $HOME/ffmpeg_bin/* /usr/bin

# INSTALL GPAC
RUN apt-get -y --force-yes install gpac

# INSTALL IMAGEMAGICK
RUN apt-get -y --force-yes install imagemagick

# CONFIGURE THE MAIN COMMAND
CMD java -jar target/website-upload-processor-1.0-SNAPSHOT.jar config/log4j.properties config/docker.properties