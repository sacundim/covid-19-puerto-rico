const core = require('@actions/core');
const github = require('@actions/github');

try {
    const owner = core.getInput('owner');
    const package = core.getInput('package');
    const per_page = core.getInput('per-page');

    const response = github.request(
        "GET /${owner}/packages/container/${package}/versions",
        { per_page: per_page }
    );

    for(version of response.data) {
        if (version.metadata.container.tags.length == 0) {
            console.log("delete " + version.id)
            /*
            const deleteResponse = github.request(
                "DELETE /${owner}/packages/container/${package}/versions/" + version.id,
                { }
            );
            console.log("status " + deleteResponse.status)
            */
        }
    }
} catch (error) {
    core.setFailed(error.message);
}
