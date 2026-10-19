"""dim_product: raw source mapping and shared Staging flow."""
import pandas as pd
from prefect import flow
from utils.quality import clean_dataframe, clean_table
from utils.schema import source_id


def build_chinook(read_ingress):
    track = read_ingress("chinook", "track")
    album = read_ingress("chinook", "album")
    artist = read_ingress("chinook", "artist")
    genre = read_ingress("chinook", "genre")

    product = track.merge(album[["albumid", "title", "artistid"]], on="albumid", how="left", suffixes=("", "_album"))
    product = product.merge(artist.rename(columns={"artistid": "artist_id", "name": "artist_name"}), left_on="artistid", right_on="artist_id", how="left")
    product = product.merge(genre.rename(columns={"genreid": "genre_id", "name": "genre_name"}), left_on="genreid", right_on="genre_id", how="left")
    product["source_system"] = "chinook"
    product["product_id"] = product["trackid"].map(lambda x: source_id("chinook", x))
    product["product_name"] = product["name"]
    dim_product = product[["source_system", "product_id", "trackid", "product_name", "genre_name", "artist_name", "title", "unitprice"]].rename(columns={"genre_name": "genre", "artist_name": "artist", "title": "album", "unitprice": "unit_price"})

    return dim_product


def build_northwind(read_ingress):
    product = read_ingress("northwind", "products")
    category = read_ingress("northwind", "categories")
    supplier = read_ingress("northwind", "suppliers")

    product = product.merge(category[["categoryid", "categoryname"]], on="categoryid", how="left")
    product = product.merge(supplier[["supplierid", "companyname"]].rename(columns={"companyname": "supplier_name"}), on="supplierid", how="left")
    product["source_system"] = "northwind"
    product["product_id"] = product["productid"].map(lambda x: source_id("northwind", x))
    dim_product = product[["source_system", "product_id", "productid", "productname", "categoryname", "supplier_name", "unitprice"]].rename(columns={"productname": "product_name", "categoryname": "category", "supplier_name": "supplier", "unitprice": "unit_price"})

    return dim_product


def stage_dataframe(dataframe):
    return clean_dataframe(dataframe, "dim_product")


@flow(name="flow_dim_product", log_prints=True, persist_result=False)
def flow_dim_product(source, storage):
    """Clean one source table; the parent flow validates and publishes the full model."""
    return clean_table(source, "dim_product", storage, processor=stage_dataframe)
