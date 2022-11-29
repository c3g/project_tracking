FROM fedora:36
ENV APP=project_tracking

RUN mkdir /app
WORKDIR /app

RUN dnf install -y python3-pip.noarch

ADD .  $APP
RUN cd $APP && pip install .

RUN flask  --app $APP init-db && flask  --app $APP add-random-project
ENTRYPOINT ["gunicorn", "project_tracking:create_app()"]
CMD ["-w", "4", "-b", "0.0.0.0" ]
