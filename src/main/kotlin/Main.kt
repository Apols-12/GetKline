package org.example

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.delay
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.io.IOException
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File
import kotlin.math.min

@Serializable
data class Kline(
   val start: Long,
   val open: Double,
   val high: Double,
   val low: Double,
   val close: Double,
   val volume: Double
)
@Serializable
data class BybitKlineResponse(
    val retCode: Int,
    val retMsg: String,
    val result: KlineResult
)
@Serializable
data class KlineResult(
    val category: String,
    val list: List<List<String>>
)

suspend fun main() {
    try {
        println("Starting historical data fetch...")
        val response = fetchHistoricalKline("SOLUSDT", "5")
        val success = saveKlineTCsv(data = response , "1yearsSolData.csv")
        if (success) {
            println("Successfully saved ${response.size} kline dada")
        }
    } catch (e: Exception) {
        println("Error fetching data: ${e.message}")
    }
}
suspend fun getKlineData(
    symbol: String,
    basUrl: String,
    category: String = "linear",
    interval: String = "5",
    start: Long,
    end: Long
    ):List<Kline> {
    val client = HttpClient {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        expectSuccess = true
        install(HttpRequestRetry) {
            maxRetries = 5
            retryOnExceptionIf { _, cause ->
                cause is ServerResponseException && cause.response.status == HttpStatusCode.TooManyRequests
            }
            delayMillis { retry -> retry * 2000L }
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 35000
            socketTimeoutMillis = 30000
        }
    }
//
    val klineResponse = client.get(basUrl) {
        parameter("category", category)
        parameter("symbol", symbol)
        parameter("interval", interval)
        parameter("start", start)
        parameter("end", end)
        parameter("limit", 1000)
    }
    val rateLimitRemaining = klineResponse.headers["X-Bapi-Limit-Status"]?.toLongOrNull() ?: 1
    val rateLimitReset = klineResponse.headers["X-Bapi-Limit-Reset-Timestamp"]?.toLongOrNull()?: 1
    if (rateLimitRemaining < 3) {
        val waitTime = (rateLimitReset*1000) - System.currentTimeMillis()
        if (waitTime > 0) {
            println("Approaching rate limit. Waiting ${waitTime}ms")
            delay(waitTime.coerceAtLeast(1000))
        }
    }
    val kline = klineResponse.body<BybitKlineResponse>()
    if (kline.retCode != 0) throw  Exception("API error: ${kline.retMsg}")
    return kline.result.list.map { item ->
        Kline(
            start = item[0].toLong(),
            open = item[1].toDouble(),
            high = item[2].toDouble(),
            low = item[3].toDouble(),
            close = item[4].toDouble(),
            volume = item[5].toDouble()

        )
    }
}
suspend fun fetchHistoricalKline(
    symbol: String,
    interval: String,
    basUrl: String = "https://api.bybit-tr.com/v5/market/kline"
) : List<Kline> {
    val start = Clock.System.now().toEpochMilliseconds() - 12*30*24*60*60*1000L
    val end = Clock.System.now().toEpochMilliseconds()
    val allEntries = mutableListOf<Kline>()
    var currentStart = start
    var requestCount = 0
    while (currentStart < end) {
        requestCount++
        val chunkEnd = min(currentStart + 1000*5*60*1000 - 1, end)
        println("Fetching chunk $requestCount: ${Instant.fromEpochMilliseconds(currentStart)} to ${Instant.fromEpochMilliseconds(chunkEnd)}")
        val chunkData = getKlineData(symbol = symbol, interval = interval, start = currentStart, end = chunkEnd, basUrl = basUrl)
        allEntries.addAll(chunkData)
        currentStart = chunkEnd + 1
        val delayTime = when {
            requestCount %20 == 0 -> 3000L
            requestCount % 5 == 0 -> 1500L
            else -> 800L
        }
        delay(delayTime)
    }
    println("Fetch ${allEntries.size} raw data")
     return allEntries
        .associateBy { it.start }
        .values
        .sortedBy { it.start }.also { println("Duplicated to ${it.size} data") }
}
fun saveKlineTCsv(data: List<Kline>, filName: String): Boolean {
    return try {
        File(filName).apply {
            parentFile?.mkdirs()
            bufferedWriter().use { writer ->
                writer.write("timestamp, open, high, low, close, volume\n")
                data.forEach { entry ->
                    val row = listOf(
                        entry.start.toString(),
                        "%.8f".format(entry.open),
                        "%.8f".format(entry.high),
                        "%.8f".format(entry.low),
                        "%.8f".format(entry.close),
                        "%.8f".format(entry.volume),
                    ).joinToString(",")
                    writer.write(row)
                    writer.newLine()
                }
            }
        }
        true
    } catch (e:IOException) {
        System.err.println("Error writing CSV file: ${e.message}")
        false
    } catch (e:Exception) {
        System.err.println("Unexpected error: ${e.message}")
        false
    }
}
