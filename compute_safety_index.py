# compute_safety_index.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def main():
    spark = SparkSession.builder.appName("TerrorismSafetyIndex").getOrCreate()

    # === CONFIGURATION ===
    INPUT_PATH = "./cleaned_terrorism_data/*.csv"  # cleaned data folder
    OUTPUT_PATH = "./output_safety_index"
    TOPK = 15

    # === READ DATA ===
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    # === SAFE CAST FUNCTION ===
    def safe_int(colname):
        """Casts a column to int safely (invalids → 0)."""
        return F.when(F.col(colname).rlike("^[0-9]+$"), F.col(colname).cast(IntegerType())).otherwise(0)

    # === PREPROCESSING ===
    processed = df.select(
        F.coalesce(F.col('country_txt'), F.lit('Unknown')).alias('country'),
        F.coalesce(safe_int('iyear'), F.lit(0)).alias('year'),
        F.coalesce(safe_int('nkill'), F.lit(0)).alias('nkill'),
        F.coalesce(safe_int('nwound'), F.lit(0)).alias('nwound'),
        F.coalesce(safe_int('success'), F.lit(0)).alias('success'),
        F.coalesce(safe_int('suicide'), F.lit(0)).alias('suicide'),
        F.coalesce(F.col('attacktype1_txt'), F.lit('Unknown')).alias('attackType'),
        F.coalesce(F.col('targtype1_txt'), F.lit('Unknown')).alias('targetType'),
        F.coalesce(F.col('weaptype1_txt'), F.lit('Unknown')).alias('weaponType'),
        F.coalesce(F.col('region_txt'), F.lit('Unknown')).alias('region'),
        (F.when(F.col('property') == '1', F.lit(1)).otherwise(F.lit(0))).alias('propertyDamage')
    ).filter((F.col('country') != 'Unknown') & (F.col('year') > 0))

    # === AGGREGATION PER COUNTRY ===
    agg = processed.groupBy('country').agg(
        F.count('*').alias('totalIncidents'),
        F.coalesce(F.sum('nkill'), F.lit(0)).alias('totalKilled'),
        F.coalesce(F.sum('nwound'), F.lit(0)).alias('totalWounded'),
        F.coalesce(F.sum('success'), F.lit(0)).alias('successfulAttacks'),
        F.coalesce(F.sum('suicide'), F.lit(0)).alias('suicideAttacks'),
        F.coalesce(F.sum('propertyDamage'), F.lit(0)).alias('propertyDamage')
    )

    # === NORMALIZATION CONSTANTS ===
    max_row = agg.agg(
        F.max('totalIncidents').alias('maxIncidents'),
        F.max('totalKilled').alias('maxKilled'),
        F.max('totalWounded').alias('maxWounded')
    ).collect()[0]

    maxIncidents = max_row['maxIncidents'] or 1
    maxKilled = max_row['maxKilled'] or 1
    maxWounded = max_row['maxWounded'] or 1

    # === SAFETY INDEX CALCULATION ===
    result = (
        agg.withColumn(
            'incidentScore', (1 - (F.col('totalIncidents') / F.lit(float(maxIncidents)))) * 30.0
        )
        .withColumn(
            'fatalityScore', (1 - (F.col('totalKilled') / F.lit(float(maxKilled)))) * 40.0
        )
        .withColumn(
            'injuryScore', (1 - (F.col('totalWounded') / F.lit(float(maxWounded)))) * 20.0
        )
        .withColumn(
            'successRateScore',
            (1 - (F.col('successfulAttacks') /
                   F.when(F.col('totalIncidents') == 0, F.lit(1)).otherwise(F.col('totalIncidents')))) * 10.0
        )
        .withColumn(
            'safetyIndex',
            F.round(
                F.col('incidentScore')
                + F.col('fatalityScore')
                + F.col('injuryScore')
                + F.col('successRateScore'),
                2,
            )
        )
        .withColumn('casualties', F.col('totalKilled') + F.col('totalWounded'))
        .withColumn(
            'attackSuccessRate',
            F.round(
                (F.col('successfulAttacks') /
                 F.when(F.col('totalIncidents') == 0, F.lit(1)).otherwise(F.col('totalIncidents'))) * 100,
                2,
            )
        )
        .withColumn(
            'riskLevel',
            F.when(F.col('safetyIndex') > 75, 'Low')
             .when(F.col('safetyIndex') > 50, 'Moderate')
             .when(F.col('safetyIndex') > 25, 'High')
             .otherwise('Critical')
        )
        .withColumn(
            'colorCode',
            F.when(F.col('riskLevel') == 'Critical', '#FF0000')
             .when(F.col('riskLevel') == 'High', '#FFA500')
             .when(F.col('riskLevel') == 'Moderate', '#FFFF00')
             .otherwise('#008000')  # Low
        )
    )

    # === OUTPUT ===
    output = result.select(
        'country', 'safetyIndex', 'riskLevel', 'colorCode',
        'totalIncidents', 'totalKilled', 'totalWounded',
        'casualties', 'attackSuccessRate', 'successfulAttacks',
        'suicideAttacks', 'propertyDamage'
    ).orderBy(F.desc('safetyIndex'))

    # Save results
    output.coalesce(1).write.mode('overwrite').option('header', True).csv(OUTPUT_PATH)
    output.limit(TOPK).coalesce(1).write.mode('overwrite').option('header', True).csv(f"{OUTPUT_PATH}/top{TOPK}")

    print(f"\n Safety Index calculation complete!\nResults saved to: {OUTPUT_PATH}\n")

    spark.stop()


if __name__ == "__main__":
    main()
