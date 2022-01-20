FROM pytorch/pytorch
CMD nvidia-smi
#set up environment
RUN apt-get update && apt-get install --no-install-recommends --no-install-suggests -y curl
RUN apt-get install -y unzip gcc g++
# Make project and results directory
RUN mkdir /app

RUN mkdir /app/notebooks
RUN mkdir /app/drivers

# Copy in the project and drivers
COPY ./notebooks/ /app/notebooks/
COPY ./drivers/ /app/drivers/

# Install dependencies and set up environmental install for drivers
RUN pip install -e /app/drivers
RUN pip install -r /app/notebooks/requirements.txt

# Add Tini. Tini operates as a process subreaper for jupyter. This prevents kernel crashes.
ENV TINI_VERSION v0.6.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini
ENTRYPOINT ["/usr/bin/tini", "--"]

EXPOSE 8888

ENTRYPOINT ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root", "--NotebookApp.token=password12","--NotebookApp.password=password12", "--notebook-dir=/app/"]