# START CREATE TOPIC
# long arguments version
mofkactl topic create collisions \
	--validator energy_validator:libenergy_validator.so \
	--validator.energy_max 100 \
	--partition-selector energy_partition_selector:libenergy_partition_selector.so \
	--partition-selector.energy_max 100 \
	--serializer energy_serializer:libenergy_serializer.so \
	--serializer.energy_max 100 \
	--groupfile mofka.ssg

# shorter version
mofkactl topic create collisions \
	-v energy_validator:libenergy_validator.so \
	--v.energy_max 100 \
	-p energy_partition_selector:libenergy_partition_selector.so \
	--p.energy_max 100 \
	-s energy_serializer:libenergy_serializer.so \
	--s.energy_max 100 \
	-g mofka.ssg
# END CREATE TOPIC

# START ADD PARTITION
# long arguments version, with memory type
mofkactl partition add collisions \
	--type memory \
	--rank 0 \
	--groupfile mofka.ssg

# long argument version, with default type
mofkactl partition add collisions \
	--type default \
	--metadata my_metadata_provider@local \
	--data my_data_provider@local \
	--rank 0 \
	--groupfile mofka.ssg

# shorter version, with default type,
# and not specifying metadata/data providers
mofkactl partition add collisions \
	-t default \
	-r 0 \
	-g mofka.ssg
# END ADD PARTITION
