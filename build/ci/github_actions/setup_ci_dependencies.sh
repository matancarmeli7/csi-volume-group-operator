#!/bin/bash -xe
set +o pipefail

staging_repository=${REGISTRY}${STAGING_REPOSITORY}
prod_repository=${REGISTRY}${PROD_REPOSITORY}
triggering_branch=$(echo $CI_ACTION_REF_NAME| sed 's|/|.|g')
image_version=${IMAGE_VERSION}
build_number=${BUILD_NUMBER}
commit_hash=${GITHUB_SHA:0:7}
specific_tag="${image_version}_b${build_number}_${commit_hash}_${triggering_branch}"
volume_group_image_specific_tag=${staging_repository}:${specific_tag}


if [ "$triggering_branch" == "develop" ]; then
  volume_group_image_branch_tag=${staging_repository}:latest
else
  volume_group_image_branch_tag=${staging_repository}:${triggering_branch}
fi

if [ "$PRODUCTION" = true ] ; then
  volume_group_image_branch_tag=${prod_repository}:${image_version}
fi

echo "volume_group_image_specific_tag=${volume_group_image_specific_tag}" >> $GITHUB_OUTPUT
echo "volume_group_image_branch_tag=${volume_group_image_branch_tag}" >> $GITHUB_OUTPUT