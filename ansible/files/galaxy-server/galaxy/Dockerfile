FROM bgruening/galaxy-stable

# Install additional requirements
COPY requirements.txt /requirements.txt
RUN . $GALAXY_VIRTUAL_ENV/bin/activate \
    && pip install -r /requirements.txt \
    && deactivate
