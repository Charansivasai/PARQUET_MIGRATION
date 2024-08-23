--Create storage intergration (Amazon S3)
create or replace storage integration HISTORY_DATA_MIGRATION
    type = external_stage
    storage_provider = s3
    enabled = TRUE
    storage_aws_role_arn = '"Amazon Resource Names"'
    storage_allowed_locations = ('"S3 url link"')
    COMMENT ='DONE';