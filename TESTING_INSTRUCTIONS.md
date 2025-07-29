# Testing Instructions for Pydantic v2 Upgrade

## Prerequisites

1. **Create a virtual environment:**
   ```bash
   python3 -m venv venv_test_pydantic
   source venv_test_pydantic/bin/activate  # On Windows: venv_test_pydantic\Scripts\activate
   ```

2. **Install the updated dagster with pydantic v2:**
   ```bash
   # Install pydantic v2 first
   pip install "pydantic>=2.0.0,<3.0.0"
   
   # Install dagster in development mode from your branch
   pip install -e python_modules/dagster/
   pip install -e python_modules/dagster-webserver/
   pip install -e python_modules/dagster-graphql/
   
   # Optional: Install libraries to test integrations
   pip install -e python_modules/libraries/dagster-snowflake/ || echo "Snowflake optional"
   pip install -e python_modules/libraries/dagster-dbt/ || echo "DBT optional"
   ```

## Testing Methods

### Method 1: Run the Comprehensive Test Script
```bash
python test_pydantic_v2.py
```

### Method 2: Manual Testing Steps

#### Step 1: Basic Import Test
```python
# Test basic imports work
python3 -c "
import pydantic
print('Pydantic version:', pydantic.__version__)

from dagster._config.pythonic_config import Config
print('✅ Dagster Config import successful')
"
```

#### Step 2: Test Config Classes
```python
python3 -c "
from dagster._config.pythonic_config import Config
from pydantic import Field, field_validator

class TestConfig(Config):
    name: str = Field(description='A name field')
    count: int = 10
    
    @field_validator('name')
    def validate_name(cls, v):
        return v.upper()

config = TestConfig(name='test')
print('✅ Config works:', config.name, config.count)
print('✅ Model fields:', list(config.model_fields.keys()))
"
```

#### Step 3: Test Asset with Config
```python
python3 -c "
from dagster import asset, materialize
from dagster._config.pythonic_config import Config

class AssetConfig(Config):
    message: str = 'Hello World'

@asset
def my_asset(config: AssetConfig):
    print('Asset config:', config.message)
    return config.message

result = materialize([my_asset])
print('✅ Asset with config executed successfully')
"
```

### Method 3: Run Existing Test Suite
```bash
# Run specific pydantic-related tests
python -m pytest python_modules/dagster/dagster_tests/core_tests/pythonic_config_tests/ -v

# Run broader config tests
python -m pytest python_modules/dagster/dagster_tests/core_tests/config_tests/ -v

# Run resource tests
python -m pytest python_modules/dagster/dagster_tests/core_tests/resource_tests/pythonic_resources/ -v
```

### Method 4: Test Library Integrations
```bash
# Test Snowflake resource (if available)
python3 -c "
try:
    from dagster_snowflake import SnowflakeResource
    resource = SnowflakeResource(account='test', user='test', password='test')
    print('✅ SnowflakeResource pydantic v2 compatible')
except ImportError:
    print('⚠️ Snowflake not installed')
except Exception as e:
    if 'pydantic' in str(e).lower():
        print('❌ Pydantic compatibility issue:', e)
    else:
        print('✅ Pydantic works (expected credential error)')
"

# Test DBT resource (if available)
python3 -c "
try:
    from dagster_dbt import DbtCliResource
    print('✅ DbtCliResource import successful')
except ImportError:
    print('⚠️ DBT not installed')
except Exception as e:
    print('❌ DBT resource error:', e)
"
```

## Expected Results

### ✅ Success Indicators:
- All imports work without pydantic-related errors
- Config classes can be instantiated and validated
- `@field_validator` decorators work correctly
- `model_fields` attribute is accessible
- Assets with config execute successfully
- No AttributeError related to `__fields__`, `__config__`, etc.

### ❌ Failure Indicators:
- Import errors mentioning pydantic compatibility
- AttributeError for `__fields__` (should be `model_fields`)
- AttributeError for `outer_type_` (should be `annotation`)
- Validator decorator errors
- Config class instantiation failures

## Common Issues and Solutions

### Issue: `AttributeError: 'TestConfig' object has no attribute '__fields__'`
**Solution:** Code still using pydantic v1 API. Check that all `__fields__` are changed to `model_fields`.

### Issue: `ImportError: cannot import name 'validator' from 'pydantic'`
**Solution:** Change `@validator` to `@field_validator` and `@root_validator` to `@model_validator`.

### Issue: `AttributeError: type object 'TestConfig' has no attribute '__config__'`
**Solution:** Change `__config__` access to `model_config`.

### Issue: Field validation not working
**Solution:** Ensure validators are using `@field_validator` with correct syntax for pydantic v2.

## Performance Testing

For production use, also test:
```bash
# Run performance benchmarks if available
python -m pytest python_modules/dagster/dagster_tests/core_tests/config_tests/test_config_performance.py -v

# Test memory usage with large configs
python3 -c "
import psutil, os
from dagster._config.pythonic_config import Config

class LargeConfig(Config):
    items: list[str] = []

process = psutil.Process(os.getpid())
memory_before = process.memory_info().rss

configs = [LargeConfig(items=[f'item_{i}' for i in range(1000)]) for _ in range(100)]

memory_after = process.memory_info().rss
print(f'Memory usage: {(memory_after - memory_before) / 1024 / 1024:.1f} MB for 100 large configs')
"
```

## Troubleshooting

If tests fail:
1. Check pydantic version: `pip show pydantic`
2. Verify all files were updated correctly
3. Look for remaining pydantic v1 patterns in the code
4. Check the migration notes in `PYDANTIC_V2_MIGRATION_NOTES.md`
5. Run with verbose error output: `python -c "import traceback; traceback.print_exc()"`