#!/bin/bash -xe
set +o pipefail

install_ci_dependencies (){
  GIT_BRANCH=$1
  IMAGE_VERSION=$2
  BUILD_NUMBER=$3
  COMMIT_HASH=${4:0:7}
  branch_image_tag=$(echo $GIT_BRANCH| sed 's|/|.|g')  #not sure if docker accept / in the version
  specific_tag="${IMAGE_VERSION}_b${BUILD_NUMBER}_${COMMIT_HASH}_${branch_image_tag}"
  echo $specific_tag
}

staging_repository=${REGISTRY}${STAGING_REPOSITORY}
prod_repository=${REGISTRY}${PROD_REPOSITORY}
triggering_branch=$(echo $CI_ACTION_REF_NAME| sed 's|/|.|g')
image_version=${IMAGE_VERSION}
build_number=${BUILD_NUMBER}
commit_hash=${GITHUB_SHA:0:7}
specific_tag="${image_version}_b${build_number}_${commit_hash}_${branch_image_tag}"
volume_group_image_specific_tag=${staging_repository}:${specific_tag}


if [ "$triggering_branch" == "develop" ]; then
  volume_group_image_branch_tag=${staging_repository}:latest
else
  volume_group_image_branch_tag=${staging_repository}:${triggering_branch}
fi

if [ "$PRODUCTION" = true ] ; then
  volume_group_image_branch_tag=${prod_repository}:${image_version}
fi

echo "::set-output name=volume_group_image_specific_tag::${volume_group_image_specific_tag}"
echo "::set-output name=volume_group_image_branch_tag::${volume_group_image_branch_tag}"