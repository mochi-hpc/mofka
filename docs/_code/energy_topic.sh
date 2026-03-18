# START CREATE TOPIC
# long arguments version
mofkactl topic create collisions \
    --validator energy_validator:libenergy_validator.so \
    --validator.energy_max 100 \
    --partition-selector energy_partition_selector:libenergy_partition_selector.so \
    --partition-selector.energy_max 100 \
    --serializer energy_serializer:libenergy_serializer.so \
    --serializer.energy_max 100 \
    --groupfile mofka.json

# shorter version
mofkactl topic create collisions \
    -v energy_validator:libenergy_validator.so \
    --v.energy_max 100 \
    -p energy_partition_selector:libenergy_partition_selector.so \
    --p.energy_max 100 \
    -s energy_serializer:libenergy_serializer.so \
    --s.energy_max 100 \
    -g mofka.json
# END CREATE TOPIC

# START ADD PARTITION
# long arguments version, with memory type
mofkactl partition add collisions \
    --type memory \
    --rank 0 \
    --groupfile mofka.json

# long argument version, with legacy type
mofkactl partition add collisions \
    --type legacy \
    --metadata my_metadata_provider@local \
    --data my_data_provider@local \
    --rank 0 \
    --groupfile mofka.json

# shorter version, with legacy type,
# and not specifying metadata/data providers
mofkactl partition add collisions \
    -t legacy \
    -r 0 \
    -g mofka.json
# END ADD PARTITION

# START ADD PROVIDERS
METADATA_PROVIDER=$(
    mofkactl metadata add \
        --rank 0 \
        --groupfile mofka.json \
        --type log \
        --config.path /tmp/mofka-log \
        --config.create_if_missing true
    )

DATA_PROVIDER=$(
    mofkactl data add \
        --rank 0 \
        --groupfile mofka.json \
        --type abtio \
        --config.path /tmp/mofka-data \
        --config.create_if_missing true
    )

mofkactl partition add collisions \
    --rank 0 \
    --groupfile mofka.json \
    --type legacy \
    --metadata "${METADATA_PROVIDER}" \
    --data "${DATA_PROVIDER}"
# END ADD PROVIDERS
