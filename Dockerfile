FROM python:3.5-onbuild

MAINTAINER lucile coutouly <lucile.coutouly@obs-nancay.fr>

ENV PYTHONUNBUFFERED 1

WORKDIR /usr/src/app

VOLUME /usr/src/app

EXPOSE 8000

ENTRYPOINT ["gunicorn", "-w 4","--log-level=debug", "-b 0.0.0.0:8000", "--reload"]

CMD ["app:app"]
