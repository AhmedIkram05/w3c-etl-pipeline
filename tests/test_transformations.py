"""
Unit tests for the transformation UDFs.

Tests the underlying logic of each UDF using PySpark's built-in
testing capabilities where needed, and direct function invocation
for pure-logic helpers.
"""

from pyspark.sql import Row

# ══════════════════════════════════════════════════════════════════════
#  page_category UDF (StringType → StringType)
# ══════════════════════════════════════════════════════════════════════


class TestPageCategory:
    """Tests the ``page_category`` UDF via direct DataFrame projection."""

    def test_aspx_dynamic(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/Darwin/MyAccount.aspx")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Dynamic Page"

    def test_html_static(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/index.html")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Static Page"

    def test_jpg_image(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/images/photo.jpg")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Image"

    def test_css_stylesheet(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/Darwin/style.css")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Stylesheet"

    def test_js_script(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/js/app.js")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Script"

    def test_ico_icon(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/favicon.ico")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Icon"

    def test_no_extension_directory(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/about/")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Directory"

    def test_root_slash(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import page_category

        df = spark.createDataFrame([Row(uri_stem="/")])
        result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
        assert result == "Directory"

    def test_unknown_values(self, spark):
        from pyspark.sql.functions import col
        from pyspark.sql.types import StringType, StructField, StructType
        from utils.transformations import page_category

        schema = StructType([StructField("uri_stem", StringType(), True)])
        for val in [None, "-", "Unknown", ""]:
            df = spark.createDataFrame([Row(uri_stem=val)], schema=schema)
            result = df.select(page_category(col("uri_stem")).alias("cat")).collect()[0]["cat"]
            assert result == "Unknown", f"Expected Unknown for {val!r}"


# ══════════════════════════════════════════════════════════════════════
#  referrer_domain UDF
# ══════════════════════════════════════════════════════════════════════


class TestReferrerDomain:
    def test_direct_traffic(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import referrer_domain

        df = spark.createDataFrame([Row(ref="-")])
        assert df.select(referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"] == "Direct"

    def test_google_referrer(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import referrer_domain

        df = spark.createDataFrame([Row(ref="http://www.google.com/search?q=darwin")])
        dom = df.select(referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"]
        assert "google" in dom

    def test_internal_referrer(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import referrer_domain

        url = "http://www.darwinsbeagleplants.org/Darwin/Plant.aspx"
        df = spark.createDataFrame([Row(ref=url)])
        dom = df.select(referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"]
        assert "darwinsbeagleplants" in dom

    def test_unknown_string(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import referrer_domain

        df = spark.createDataFrame([Row(ref="Unknown")])
        assert df.select(referrer_domain(col("ref")).alias("dom")).collect()[0]["dom"] == "Unknown"


# ══════════════════════════════════════════════════════════════════════
#  traffic_type UDF
# ══════════════════════════════════════════════════════════════════════


class TestTrafficType:
    def test_direct(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import traffic_type

        df = spark.createDataFrame([Row(ref="-")])
        assert df.select(traffic_type(col("ref")).alias("t")).collect()[0]["t"] == "Direct"

    def test_search_engine(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import traffic_type

        df = spark.createDataFrame([Row(ref="http://www.google.com/search?q=test")])
        assert df.select(traffic_type(col("ref")).alias("t")).collect()[0]["t"] == "Search Engine"

    def test_social(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import traffic_type

        df = spark.createDataFrame([Row(ref="http://www.facebook.com/post/123")])
        assert df.select(traffic_type(col("ref")).alias("t")).collect()[0]["t"] == "Social"

    def test_internal(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import traffic_type

        df = spark.createDataFrame([Row(ref="http://www.darwinsbeagleplants.org/page")])
        assert df.select(traffic_type(col("ref")).alias("t")).collect()[0]["t"] == "Internal"

    def test_referral(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import traffic_type

        df = spark.createDataFrame([Row(ref="http://some-other-site.com/link")])
        assert df.select(traffic_type(col("ref")).alias("t")).collect()[0]["t"] == "Referral"


# ══════════════════════════════════════════════════════════════════════
#  make_crawler_udf
# ══════════════════════════════════════════════════════════════════════


class TestMakeCrawlerUDF:
    def test_known_crawler(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import make_crawler_udf

        crawler_set = {"66.249.66.87", "207.46.199.39"}
        is_crawler = make_crawler_udf(crawler_set)
        df = spark.createDataFrame([Row(ip="66.249.66.87")])
        assert df.select(is_crawler(col("ip")).alias("c")).collect()[0]["c"] == "true"

    def test_not_crawler(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import make_crawler_udf

        crawler_set = {"66.249.66.87"}
        is_crawler = make_crawler_udf(crawler_set)
        df = spark.createDataFrame([Row(ip="1.2.3.4")])
        assert df.select(is_crawler(col("ip")).alias("c")).collect()[0]["c"] == "false"

    def test_dash_ip(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import make_crawler_udf

        is_crawler = make_crawler_udf(set())
        df = spark.createDataFrame([Row(ip="-")])
        assert df.select(is_crawler(col("ip")).alias("c")).collect()[0]["c"] == "Unknown"

    def test_none_ip(self, spark):
        from pyspark.sql.functions import col
        from pyspark.sql.types import StringType, StructField, StructType
        from utils.transformations import make_crawler_udf

        is_crawler = make_crawler_udf(set())
        schema = StructType([StructField("ip", StringType(), True)])
        df = spark.createDataFrame([Row(ip=None)], schema=schema)
        assert df.select(is_crawler(col("ip")).alias("c")).collect()[0]["c"] == "Unknown"


# ══════════════════════════════════════════════════════════════════════
#  size_band UDF
# ══════════════════════════════════════════════════════════════════════


class TestSizeBand:
    def test_zero(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import size_band

        df = spark.createDataFrame([Row(sent=0, recv=0)])
        assert df.select(size_band(col("sent"), col("recv")).alias("b")).collect()[0]["b"] == "Zero"

    def test_tiny(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import size_band

        df = spark.createDataFrame([Row(sent=500, recv=0)])
        assert df.select(size_band(col("sent"), col("recv")).alias("b")).collect()[0]["b"] == "Tiny"

    def test_small(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import size_band

        df = spark.createDataFrame([Row(sent=2000, recv=0)])
        assert df.select(size_band(col("sent"), col("recv")).alias("b")).collect()[0]["b"] == "Small"

    def test_medium(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import size_band

        df = spark.createDataFrame([Row(sent=50000, recv=0)])
        assert df.select(size_band(col("sent"), col("recv")).alias("b")).collect()[0]["b"] == "Medium"

    def test_large(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import size_band

        df = spark.createDataFrame([Row(sent=500000, recv=0)])
        assert df.select(size_band(col("sent"), col("recv")).alias("b")).collect()[0]["b"] == "Large"

    def test_huge(self, spark):
        from pyspark.sql.functions import col
        from utils.transformations import size_band

        df = spark.createDataFrame([Row(sent=2_000_000, recv=0)])
        assert df.select(size_band(col("sent"), col("recv")).alias("b")).collect()[0]["b"] == "Huge"
