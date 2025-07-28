from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('food', '0136_merge_20250428_0153'),  # your latest migration filename here
    ]

    operations = [
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS food_restaurant_name_trgm_idx ON food_restaurant USING gin (name gin_trgm_ops);",
            "DROP INDEX IF EXISTS food_restaurant_name_trgm_idx;"
        ),
        migrations.RunSQL(
            "CREATE INDEX IF NOT EXISTS food_location_name_trgm_idx ON food_location USING gin (name gin_trgm_ops);",
            "DROP INDEX IF EXISTS food_location_name_trgm_idx;"
        ),
    ]
