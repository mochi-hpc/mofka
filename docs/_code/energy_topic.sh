# long arguments version
mofkactl topic create energy \
	--validator energy_validator:libenergy_validator.so \
	--validator.energy_max 100 \
	--partition-selector energy_partition_selector:libenergy_partition_selector.so \
	--partition-selector.energy_max 100 \
	--serializer energy_serializer:libenergy_serializer.so \
	--serializer.energy_max 100 \
	--groupfile mofka.ssg

# shorter version
mofkactl topic create energy \
	--v energy_validator:libenergy_validator.so \
	--v.energy_max 100 \
	--p energy_partition_selector:libenergy_partition_selector.so \
	--p.energy_max 100 \
	--s energy_serializer:libenergy_serializer.so \
	--s.energy_max 100 \
	--g mofka.ssg
