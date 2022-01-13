from PIL import Image, ImageOps
from urllib import parse
import boto3
import base64
import io

s3_bucket_name = "taxijjang-sample-s3"
s3_client = boto3.client("s3", "ap-northeast-2")


def get_s3_object(s3_object_key):
    """
    s3 object key 
    """
    try:
        return s3_client.get_object(
            Bucket=s3_bucket_name, 
            Key=parse.unquote(s3_object_key)
        )
    except Exception as e:
        print("get_s3_object Exception", e)


def get_image_spec(query):
    """
    query params로 받은 정보 알맞게 추출
    """
    params = {k: v[0] for k, v in parse.parse_qs(query.lower()).items()}
    width = int(params.get('w', 1080))
    height = int(params.get('h', 1080))
    quality = int(params.get('q', 80))
    return (width, height, quality)


def resize_image(original_image, image_spec):
    """
    get_image_spec에서 알맞게 추출된 spec을 이용하여 이미지 변환
    """
    width, height, quality = image_spec
    try:
        fixed_image = ImageOps.exif_transpose(original_image)
        fixed_image.thumbnail((width, height), Image.LANCZOS)
        bytes_io = io.BytesIO()
        fixed_image.save(bytes_io, format=original_image.format, optimize=True, quality=quality)

        original_image.close()

        result_size = bytes_io.tell()
        result_data = bytes_io.getvalue()
        result = base64.standard_b64encode(result_data).decode()
        bytes_io.close()
        res = {
            'resized_image': result,
            'resized_image_size': result_size
        }
        return res
    except Exception as e:
        print(e)


def lambda_handler(event, context):
    request = event["Records"][0]["cf"]["request"]
    response = event["Records"][0]["cf"]["response"]

    # 정상 response 오지 않을 때
    if int(response.get('status')) != 200:
        return response

    query = request['querystring']
    uri = request['uri']
    s3_object_key = uri[1:]

    s3_response = get_s3_object(s3_object_key)
    
    if not s3_response:
        return response
    s3_object_type = s3_response["ContentType"]

    # 올바른 이미지 규격이 아닐 때
    if s3_object_type not in ["image/jpeg", "image/png", "image/jpg"]:
        return response

    image_spec = get_image_spec(query)
    original_image = Image.open(s3_response["Body"])
    result = resize_image(original_image, image_spec)

    if not result or result.get('resized_image_size') > 1024 * 1024:
        return response
    
    # image의 파일 용량은 1mb
    response["status"] = 200
    response["statusDescription"] = "OK"
    response["body"] = result.get('resized_image')
    response["bodyEncoding"] = "base64"
    response["headers"]["content-type"] = [
        {"key": "Content-Type", "value": s3_object_type}
    ]
    return response
