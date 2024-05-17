from pyspark.ml.feature import VectorAssembler, StandardScaler
from logger import Logger


SHOW_LOG = True


class Preprocess:
    def __init__(self):
        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

    def load_dataset(self, path_to_data, spark):

        dataset = spark.read.csv(
            path_to_data,
            header=True,
            inferSchema=True,
            sep='\t',
        )

        dataset.fillna(value=0)

        output_col = 'features'
        vector_assembler = VectorAssembler(
            inputCols=[
                'completeness',
                'energy-kcal_100g',
                'energy_100g',
                'fat_100g',
                'saturated-fat_100g',
                'carbohydrates_100g',
                'sugars_100g',
                'proteins_100g',
                'salt_100g',
                'sodium_100g'
            ],
            outputCol=output_col,
            handleInvalid='skip',
        )

        assembled_data = vector_assembler.transform(dataset)
        self.log.info("Dataset READY")

        return assembled_data

    
    def std_assembled_dataset(self, assembled_data):
        stdize = StandardScaler(
            inputCol='features',
            outputCol='stdized_features'
        )
        stdize_model = stdize.fit(assembled_data)
        stdize_data = stdize_model.transform(assembled_data)
        self.log.info("Data standardized with mean removimg and scaling to std deviation")

        return stdize_data