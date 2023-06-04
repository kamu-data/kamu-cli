publish:
	aws s3 rm --recursive s3://get.kamu.dev
	aws s3 cp kamu-install.sh s3://get.kamu.dev/kamu-install.sh
	aws cloudfront create-invalidation --distribution-id E2KGCU2ISYOYUU --paths '/*'
