FROM fedora:36
ENV APP=c3g_project_tracking

RUN mkdir /app
WORKDIR /app

RUN dnf install -y python3-pip.noarch

ADD .  $APP
RUN cd $APP && pip install .

RUN flask  --app c3g_project_tracking init-db && flask  --app c3g_project_tracking add-random-project
ENTRYPOINT ["gunicorn", "c3g_project_tracking:create_app()"]
CMD ["-w", "4", "-b", "0.0.0.0" ]
