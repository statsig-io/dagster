# Pydantic v2 Migration Notes

This document tracks the migration from pydantic v1 to v2.

## Completed Changes

### Core Configuration Files
- `python_modules/dagster/setup.py`: Updated pydantic version constraint to `>=2.0.0,<3.0.0`
- `helm/dagster/schema/setup.py`: Updated pydantic version constraint 
- `pyright/master/requirements-pinned.txt`: Updated to pydantic==2.10.4
- `pyright/alt-1/requirements-pinned.txt`: Updated to pydantic==2.10.4

### Code Updates
- `python_modules/dagster/dagster/_config/pythonic_config/config.py`:
  - Changed `from pydantic import Extra` to `from pydantic import ConfigDict`
  - Updated `class Config:` to `model_config = ConfigDict()`
  - Changed `__fields__` to `model_fields`
  - Changed `__config__.extra` to `model_config.get('extra')`
  - Changed `ModelField` to `FieldInfo`

- `python_modules/dagster/dagster/_config/pythonic_config/resource.py`:
  - Changed `__fields__` to `model_fields`
  - Changed `outer_type_` to `annotation`

- `python_modules/dagster/dagster/_config/pythonic_config/conversion_utils.py`:
  - Updated imports for pydantic v2
  - Changed `ModelField` to `FieldInfo`
  - Updated some field attribute access patterns

### Test Files
- `dagster_tests/core_tests/pythonic_config_tests/test_validation.py`:
  - Changed `@validator` to `@field_validator`

### Library Files  
- `python_modules/libraries/dagster-snowflake/dagster_snowflake/resources.py`:
  - Changed `@validator` to `@field_validator`
  - Changed `@root_validator` to `@model_validator(mode='before')`

- `python_modules/libraries/dagster-dbt/dagster_dbt/core/resources_v2.py`:
  - Changed `@validator` to `@field_validator`
  - Updated `pre=True` to `mode='before'`

## Still Needs Work

### Complex Field Access Patterns
The following areas in `conversion_utils.py` still need more work:

1. **Field shape handling**: Pydantic v2 changed how field shapes are represented and accessed
2. **Key field access**: `pydantic_field.key_field` may not exist in v2
3. **Discriminator handling**: `pydantic_field.field_info.discriminator` has changed
4. **Allow none detection**: `pydantic_field.allow_none` needs new approach
5. **Default value handling**: Default value access patterns have changed

### Validation Functions
Some validation functions may need updates for v2 API changes.

### Testing Required
- Full test suite execution to identify remaining compatibility issues
- Integration testing with actual pydantic v2 installation

## Migration Strategy
This is a partial migration to get basic compatibility. Further work will be needed to fully leverage pydantic v2 features and fix any remaining issues discovered through testing.