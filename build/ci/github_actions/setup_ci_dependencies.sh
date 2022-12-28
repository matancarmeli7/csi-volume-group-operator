#!/bin/bash -xe
set +o pipefail

triggering_branch=$(echo $CI_ACTION_REF_NAME| sed 's|/|.|g')
image_version=${IMAGE_VERSION}
build_number=${BUILD_NUMBER}
commit_hash=${GITHUB_SHA:0:7}
specific_tag="${image_version}_b${build_number}_${commit_hash}_${triggering_branch}"


if [ "$triggering_branch" == "develop" ]; then
  branch_tag=latest
else
  branch_tag=${triggering_branch}
fi

if [ "$PRODUCTION" = true ]; then
  repository=${PROD_REPOSITORY}
  branch_tag=${image_version}
else
  repository=${STAGING_REPOSITORY}
fi

#echo "volume_group_image_specific_tag=${volume_group_image_specific_tag}" >> $GITHUB_OUTPUT
#echo "volume_group_image_branch_tag=${volume_group_image_branch_tag}" >> $GITHUB_OUTPUT
echo "repository=${repository}" >> $GITHUB_OUTPUT
echo "specific_tag=${specific_tag}" >> $GITHUB_OUTPUT
echo "branch_tag=${branch_tag}" >> $GITHUB_OUTPUT