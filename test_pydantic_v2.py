#!/usr/bin/env python3
"""
Test script for pydantic v2 migration in Dagster.
This script tests various aspects of the pydantic v2 upgrade.
"""

def test_basic_imports():
    """Test that basic imports work with pydantic v2."""
    print("=== Testing Basic Imports ===")
    
    try:
        import pydantic
        print(f"✅ Pydantic version: {pydantic.__version__}")
        assert pydantic.__version__.startswith('2.'), f"Expected pydantic v2, got {pydantic.__version__}"
    except Exception as e:
        print(f"❌ Pydantic import failed: {e}")
        return False
    
    try:
        from dagster._config.pythonic_config import Config, PermissiveConfig
        print("✅ Dagster Config imports successful")
    except Exception as e:
        print(f"❌ Dagster Config import failed: {e}")
        return False
    
    return True

def test_config_classes():
    """Test that Config classes work with pydantic v2."""
    print("\n=== Testing Config Classes ===")
    
    try:
        from dagster._config.pythonic_config import Config
        from pydantic import Field, field_validator
        
        class TestConfig(Config):
            name: str = "default"
            count: int = Field(default=10, description="A count field")
            
            @field_validator("name")
            def validate_name(cls, v):
                if not v:
                    raise ValueError("Name cannot be empty")
                return v.upper()
        
        # Test basic instantiation
        config = TestConfig(name="test", count=5)
        print(f"✅ Config creation successful: {config.name}, {config.count}")
        
        # Test validation
        try:
            TestConfig(name="", count=5)
            print("❌ Validation should have failed for empty name")
            return False
        except ValueError:
            print("✅ Field validation works correctly")
        
        # Test model_fields access (pydantic v2)
        fields = TestConfig.model_fields
        print(f"✅ model_fields access works: {list(fields.keys())}")
        
        return True
        
    except Exception as e:
        print(f"❌ Config class test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_permissive_config():
    """Test PermissiveConfig with pydantic v2."""
    print("\n=== Testing PermissiveConfig ===")
    
    try:
        from dagster._config.pythonic_config import PermissiveConfig
        
        class TestPermissiveConfig(PermissiveConfig):
            required_field: str
        
        # Test with extra fields
        config = TestPermissiveConfig(
            required_field="test",
            extra_field="extra_value"
        )
        print("✅ PermissiveConfig allows extra fields")
        
        # Access extra fields
        config_dict = config.model_dump()
        print(f"✅ Extra field accessible: {config_dict.get('extra_field')}")
        
        return True
        
    except Exception as e:
        print(f"❌ PermissiveConfig test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_validators():
    """Test that validators work with pydantic v2."""
    print("\n=== Testing Validators ===")
    
    try:
        from dagster._config.pythonic_config import Config
        from pydantic import field_validator, ValidationError
        
        class ValidatedConfig(Config):
            username: str
            age: int
            
            @field_validator("username")
            def validate_username(cls, v):
                if not v.isalnum():
                    raise ValueError("Username must be alphanumeric")
                return v.lower()
            
            @field_validator("age")
            def validate_age(cls, v):
                if v < 0:
                    raise ValueError("Age must be positive")
                return v
        
        # Test valid config
        config = ValidatedConfig(username="TestUser123", age=25)
        print(f"✅ Valid config: {config.username}, {config.age}")
        
        # Test validation failures
        try:
            ValidatedConfig(username="test user!", age=25)
            print("❌ Should have failed for invalid username")
            return False
        except ValidationError:
            print("✅ Username validation works")
        
        try:
            ValidatedConfig(username="testuser", age=-5)
            print("❌ Should have failed for negative age")
            return False
        except ValidationError:
            print("✅ Age validation works")
        
        return True
        
    except Exception as e:
        print(f"❌ Validator test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_library_imports():
    """Test that library integrations work."""
    print("\n=== Testing Library Imports ===")
    
    # Test snowflake resource
    try:
        from dagster_snowflake import SnowflakeResource
        print("✅ SnowflakeResource import successful")
        
        # Test that it can be instantiated (will fail without credentials, but import/validation should work)
        try:
            SnowflakeResource(
                account="test",
                user="test",
                password="test"
            )
            print("✅ SnowflakeResource instantiation successful")
        except Exception as e:
            # Expected to fail without real credentials, but should not be a pydantic error
            if "pydantic" in str(e).lower():
                print(f"❌ Pydantic-related error in SnowflakeResource: {e}")
                return False
            else:
                print(f"✅ SnowflakeResource validation works (expected credential error: {type(e).__name__})")
    
    except ImportError:
        print("⚠️  SnowflakeResource not available (optional dependency)")
    except Exception as e:
        print(f"❌ SnowflakeResource test failed: {e}")
        return False
    
    # Test DBT resource
    try:
        from dagster_dbt import DbtCliResource
        print("✅ DbtCliResource import successful")
    except ImportError:
        print("⚠️  DbtCliResource not available (optional dependency)")
    except Exception as e:
        print(f"❌ DbtCliResource import failed: {e}")
        return False
    
    return True

def test_asset_with_config():
    """Test that assets with config work."""
    print("\n=== Testing Asset with Config ===")
    
    try:
        from dagster import asset, materialize
        from dagster._config.pythonic_config import Config
        from pydantic import Field
        
        class AssetConfig(Config):
            param: str = Field(description="A parameter")
            count: int = 5
        
        @asset
        def test_asset(config: AssetConfig) -> str:
            return f"Asset executed with {config.param} and count {config.count}"
        
        # Test materialization with config
        result = materialize(
            [test_asset],
            run_config={
                "ops": {
                    "test_asset": {
                        "config": {
                            "param": "test_value",
                            "count": 10
                        }
                    }
                }
            }
        )
        
        print("✅ Asset with pydantic v2 config executed successfully")
        return True
        
    except Exception as e:
        print(f"❌ Asset with config test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all tests."""
    print("Testing Pydantic v2 Migration in Dagster")
    print("=" * 50)
    
    tests = [
        test_basic_imports,
        test_config_classes, 
        test_permissive_config,
        test_validators,
        test_library_imports,
        test_asset_with_config,
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            failed += 1
    
    print(f"\n{'=' * 50}")
    print(f"Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Pydantic v2 migration appears successful.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit(main())