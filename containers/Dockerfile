FROM condaforge/mambaforge

ARG HOST_UID
ARG HOST_GID
ARG SERVICES
ARG ENV
ENV ENV_NAME=crawlclima
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  build-essential \
  ca-certificates sudo curl cron vim \
  && rm -rf /var/lib/apt/lists/*

# Create epiuser user
RUN addgroup --gid ${HOST_GID} epiuser \
  && useradd --uid ${HOST_UID} --gid ${HOST_GID} -ms /bin/bash epiuser \
  && echo "epiuser ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/epiuser \
  && chmod 0440 /etc/sudoers.d/epiuser \
  && export ENV_NAME="$ENV_NAME" \
  && mkdir -p /opt/conda /opt/services/logs /opt/services/crawlclima \
  && chmod -R a+rwx /opt/conda /opt/services/crawlclima/ /opt/services/logs /tmp \
  && chown epiuser:epiuser /opt/services/ /tmp \
  && echo 'source /opt/conda/bin/activate "$ENV_NAME" && exec "$@"' > /activate.sh \
  && echo 'source activate "$ENV_NAME"' >  /home/epiuser/.bashrc \
  && chmod +x /activate.sh

ENV PATH "$PATH:/home/epiuser/.local/bin"
ENV PATH /opt/conda/envs/$ENV_NAME/bin:$PATH


# Configure cron for DBF request
COPY --chown=epiuser:epiuser crawlclima/cron_tasks /etc/cron.d/cron_tasks

RUN chmod 0644  /etc/cron.d/cron_tasks \
  && crontab -u epiuser  /etc/cron.d/cron_tasks  \
  && echo "epiuser"  > /etc/cron.allow \
  && touch /var/log/cron.log \
  && touch /var/run/crond.pid \
  && chown epiuser:epiuser /var/log/cron.log \
  && chown epiuser:epiuser /var/run/crond.pid

USER epiuser

## Copy code to the container with permissions to user
COPY --chown=epiuser:epiuser pyproject.toml poetry.lock README.md /opt/services/
COPY --chown=epiuser:epiuser conda/dev.yaml /tmp/environment.yaml
COPY --chown=epiuser:epiuser containers/scripts/poetry-install.sh /tmp/poetry-install.sh

COPY --chown=epiuser:epiuser crawlclima /opt/services/crawlclima

# Copy the script file to initialize cron and celery
COPY --chown=epiuser:epiuser containers/scripts/start_cron_celery.sh /start_cron_celery.sh
RUN chmod +x /start_cron_celery.sh

# Change shell to bash
SHELL ["/bin/bash", "-c"]

# Use environment to update the env crawlclima
RUN mamba env create -n crawlclima --file /tmp/environment.yaml --force \
  && cd /opt/services/ \
  && . /tmp/poetry-install.sh \
  && conda clean --all \
  && find /opt/conda/ -type f,l -name '*.a' -delete \
  && find /opt/conda/ -type f,l -name '*.pyc' -delete \
  && find /opt/conda/ -type f,l -name '*.js.map' -delete \
  && rm -rf /opt/conda/pkgs

WORKDIR /opt/services/

ENTRYPOINT ["bash", "/activate.sh"]
CMD ["/start_cron_celery.sh"]
