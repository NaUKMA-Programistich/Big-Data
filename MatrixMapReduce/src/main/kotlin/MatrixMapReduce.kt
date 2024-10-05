import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class MatrixMapReduce {
    companion object {
        private const val MATRIX_A_COLUMN_B_ROW = "matrix.a.column.b.row"
        const val MATRIX_A_ROW = "matrix.a.row"
        const val MATRIX_B_COLUMN = "matrix.b.column"

        private val logger = LoggerFactory.getLogger(MatrixMapReduce::class.java)

        @JvmStatic
        fun main(args: Array<String>) {
            if (args.size != 6) {
                println("Usage: MatrixMapReduce <matrixA_path> <matrixB_path> <output_path> <matrixA_rows> <matrixA_columns/matrixB_rows> <matrixB_columns>")
                exitProcess(1)
            }

            logger.info("MatrixMapReduce main \n${args.joinToString("\n")}")

            val input = Path(args[1])
            val output = Path(args[2])

            val matrixARow = Integer.parseInt(args[3])
            val matrixAColumnBRow = Integer.parseInt(args[4])
            val matrixBColumn = Integer.parseInt(args[5])

            logger.info("Matrix A: $matrixARow x $matrixAColumnBRow")
            logger.info("Matrix B: $matrixAColumnBRow x $matrixBColumn")

            logger.info("Input: $input")
            logger.info("Output: $output")

            val configuration = Configuration()

            configuration.setInt(MATRIX_A_ROW, matrixARow)
            configuration.setInt(MATRIX_A_COLUMN_B_ROW, matrixAColumnBRow)
            configuration.setInt(MATRIX_B_COLUMN, matrixBColumn)

            val job = Job.getInstance(configuration, "MatrixMapReduce").apply {
                setJarByClass(MatrixMapReduce::class.java)

                mapperClass = MatrixMapper::class.java
                reducerClass = MatrixReducer::class.java

                mapOutputKeyClass = Text::class.java
                mapOutputValueClass = Text::class.java

                outputKeyClass = Text::class.java
                outputValueClass = DoubleWritable::class.java
            }

            FileInputFormat.addInputPath(job, Path(input, "first.txt"))
            FileInputFormat.addInputPath(job, Path(input, "second.txt"))
            FileOutputFormat.setOutputPath(job, output)

            exitProcess(if (job.waitForCompletion(true)) 0 else 1)
        }
    }
}

class MatrixMapper : Mapper<LongWritable, Text, Text, Text>() {

    private val logger = LoggerFactory.getLogger(MatrixMapper::class.java)

    override fun map(key: LongWritable, value: Text, context: Context) {
        val tokens = value.toString().trim().split(",")

        if (tokens.size != 4) {
            logger.warn("Invalid input line: {}", value)
            return
        }

        val (matrixId, i, j, vStr) = tokens.map { it.trim() }
        val currentValue = vStr.toDoubleOrNull() ?: run {
            logger.warn("Invalid numeric value: {}", vStr)
            return
        }

        when (matrixId) {
            "A" -> emitForMatrixA(i, j, currentValue, context)
            "B" -> emitForMatrixB(i, j, currentValue, context)
            else -> logger.warn("Unknown matrix identifier: {}", matrixId)
        }
    }

    private fun emitForMatrixA(i: String, j: String, currentValue: Double, context: Context) {
        val bColumns = context.configuration.getInt(MatrixMapReduce.MATRIX_B_COLUMN, 0)
        if (bColumns == 0) {
            logger.warn("Matrix B column size is not set")
            return
        }

        repeat(bColumns) { col ->
            val outputKey = "$i,${col + 1}"
            val outputValue = "A,$j,$currentValue"
            context.write(Text(outputKey), Text(outputValue))
        }
    }

    private fun emitForMatrixB(i: String, j: String, currentValue: Double, context: Context) {
        val aRows = context.configuration.getInt(MatrixMapReduce.MATRIX_A_ROW, 0)
        if (aRows == 0) {
            logger.warn("Matrix A row size is not set")
            return
        }

        repeat(aRows) { row ->
            val outputKey = "${row + 1},$j"
            val outputValue = "B,$i,$currentValue"
            context.write(Text(outputKey), Text(outputValue))
        }
    }
}


class MatrixReducer : Reducer<Text, Text, Text, DoubleWritable>() {
    private val logger = LoggerFactory.getLogger(MatrixReducer::class.java)

    override fun reduce(key: Text, values: Iterable<Text>, context: Context) {
        val aMap = mutableMapOf<String, Double>()
        val bMap = mutableMapOf<String, Double>()

        values.forEach { value ->
            val tokens = value.toString().split(",")
            if (tokens.size != 3) {
                logger.warn("Invalid value format: $value")
                return@forEach
            }

            val (matrixId, k, vStr) = tokens.map { it.trim() }
            val v = vStr.toDoubleOrNull() ?: run {
                logger.warn("Invalid numeric value in: $value")
                return@forEach
            }

            when (matrixId) {
                "A" -> aMap[k] = v
                "B" -> bMap[k] = v
                else -> logger.warn("Unknown matrix identifier in value: $value")
            }
        }

        val sum = aMap.entries.sumOf { (k, aValue) -> bMap[k]?.let { aValue * it } ?: 0.0 }
        context.write(key, DoubleWritable(sum))
    }
}
