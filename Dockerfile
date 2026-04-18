FROM ubuntu:22.04

ARG MINIFORGE_VERSION=26.1.1-3

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libarchive-dev \
        wget \
    && rm -rf /var/lib/apt/lists/*

ENV CONDA_DIR=/opt/conda
RUN wget -q "https://github.com/conda-forge/miniforge/releases/download/${MINIFORGE_VERSION}/Miniforge3-Linux-x86_64.sh" -O /tmp/miniforge.sh \
    && bash /tmp/miniforge.sh -b -p "${CONDA_DIR}" \
    && rm /tmp/miniforge.sh
ENV PATH=${CONDA_DIR}/bin:${PATH}

RUN mamba create -y -n rdkit -c conda-forge \
        "rdkit=2019.09.3=py38hb31dc5d_0" \
        "boost=1.70.0" \
        "python=3.8" \
    && /bin/bash -lc "source activate rdkit && pip install --no-cache-dir 'numpy<2' h5py pandas pyarrow requests tqdm zstandard" \
    && mamba clean -afy

ENV OPENBLAS_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1
ENV NPC_WORK_DIR=/work

WORKDIR /app
COPY . /app
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["run"]
