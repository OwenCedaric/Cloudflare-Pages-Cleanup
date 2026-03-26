const { backOff } = require('exponential-backoff')

const CF_API_TOKEN = process.env.CF_API_TOKEN
const CF_ACCOUNT_ID = process.env.CF_ACCOUNT_ID
const CF_PAGES_PROJECT_NAME = process.env.CF_PAGES_PROJECT_NAME
const CF_DELETE_ALIASED_DEPLOYMENTS = process.env.CF_DELETE_ALIASED_DEPLOYMENTS

const MAX_ATTEMPTS = 5

const DEPLOYMENTS_PER_PAGE = 25
const PAGINATION_BATCH_SIZE = 4
const BATCH_MAX_RESULTS = PAGINATION_BATCH_SIZE * DEPLOYMENTS_PER_PAGE

// --- 配置常量 ---
const DELAY_BETWEEN_CHUNKS_MS = 500
const DELAY_BETWEEN_PAGES_MS = 500
const DELETE_CONCURRENCY_LIMIT = 3 // 每次并发删除的数量

const sleep = (ms) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms)
  })

const headers = {
  Authorization: `Bearer ${CF_API_TOKEN}`,
}

/** Get the canonical deployment (the live deployment) */
async function getProductionDeploymentId() {
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/pages/projects/${CF_PAGES_PROJECT_NAME}`,
    {
      method: 'GET',
      headers,
    }
  )
  const body = await response.json()
  if (!body.success) {
    throw new Error(body.errors[0].message)
  }
  const prodDeploymentId = body.result.canonical_deployment?.id
  if (!prodDeploymentId) {
    return null;
  }
  return prodDeploymentId
}

/** * Delete a specific deployment
 * 增加了指数退避重试机制，以应对网络抖动或 API 限流 (HTTP 429)
 */
async function deleteDeployment(id) {
  let params = ''
  if (CF_DELETE_ALIASED_DEPLOYMENTS === 'true') {
    params = '?force=true' // Forces deletion of aliased deployments
  }

  await backOff(
    async () => {
      const response = await fetch(
        `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/pages/projects/${CF_PAGES_PROJECT_NAME}/deployments/${id}${params}`,
        {
          method: 'DELETE',
          headers,
        }
      )
      const body = await response.json()
      if (!body.success) {
        throw new Error(body.errors[0].message)
      }
    },
    {
      numOfAttempts: MAX_ATTEMPTS,
      startingDelay: 1000,
      retry: (err, attempt) => {
        console.warn(
          `Failed to delete deployment ${id}... retrying (${attempt}/${MAX_ATTEMPTS}). Error: ${err.message}`
        )
        return true
      },
    }
  )

  console.log(`Deleted deployment ${id} for project ${CF_PAGES_PROJECT_NAME}`)
}

async function listDeploymentsPerPage(page) {
  const response = await fetch(
    `https://api.cloudflare.com/client/v4/accounts/${CF_ACCOUNT_ID}/pages/projects/${CF_PAGES_PROJECT_NAME}/deployments?per_page=${DEPLOYMENTS_PER_PAGE}&page=${page}`,
    {
      method: 'GET',
      headers,
    }
  )
  const body = await response.json()
  if (!body.success) {
    throw new Error(`Could not fetch deployments for ${CF_PAGES_PROJECT_NAME}`)
  }

  if(body.result?.length){
    const amountOfDeploymentsFound = (page-1) * DEPLOYMENTS_PER_PAGE + body.result?.length
    console.log(`Fetching deployments... (${amountOfDeploymentsFound} deployments fetched)`)
  }

  return body.result
}

async function listNextDeployments() {
  let page = 1
  const deploymentIds = []

  console.log(`Listing next ${BATCH_MAX_RESULTS} deployments, this may take a while...`)
  while (true) {
    let result
    try {
      result = await backOff(() => listDeploymentsPerPage(page), {
        numOfAttempts: MAX_ATTEMPTS,
        startingDelay: 1000, 
        retry: (_, attempt) => {
          console.warn(
            `Failed to list deployments on page ${page}... retrying (${attempt}/${MAX_ATTEMPTS})`
          )
          return true
        },
      })
    } catch (err) {
      console.warn(`Failed to list deployments on page ${page}.`)
      console.warn(err)
      process.exit(1)
    }

    for (const deployment of result) {
      deploymentIds.push(deployment.id)
    }

    if (result.length && (BATCH_MAX_RESULTS > page * DEPLOYMENTS_PER_PAGE)) {
      page = page + 1
      await sleep(DELAY_BETWEEN_PAGES_MS)
    } else {
      return deploymentIds
    }
  }
}

/** * 批量删除部署
 * 采用分块并发 (Chunking Concurrency) 提升效率
 */
async function deleteBatch(deploymentIds, productionDeploymentId) {
  // 过滤出真正需要删除的 ID
  const idsToDelete = deploymentIds.filter((id) => {
    if (productionDeploymentId !== null && id === productionDeploymentId) {
      console.log(`Skipping production deployment: ${id}`)
      return false
    }
    return true
  })

  // 以 DELETE_CONCURRENCY_LIMIT 为步长，对任务进行分块并发
  for (let i = 0; i < idsToDelete.length; i += DELETE_CONCURRENCY_LIMIT) {
    const chunk = idsToDelete.slice(i, i + DELETE_CONCURRENCY_LIMIT)

    await Promise.allSettled(
      chunk.map(async (id) => {
        try {
          await deleteDeployment(id)
        } catch (error) {
          console.error(`Final error deleting ${id}:`, error.message)
        }
      })
    )

    // 块与块之间进行延时，防止瞬间请求过多触发 API 速率限制
    if (i + DELETE_CONCURRENCY_LIMIT < idsToDelete.length) {
      await sleep(DELAY_BETWEEN_CHUNKS_MS)
    }
  }
}

async function main() {
  if (!CF_API_TOKEN) {
    throw new Error('Please set CF_API_TOKEN as an env variable to your API Token')
  }

  if (!CF_ACCOUNT_ID) {
    throw new Error('Please set CF_ACCOUNT_ID as an env variable to your Account ID')
  }

  if (!CF_PAGES_PROJECT_NAME) {
    throw new Error(
      'Please set CF_PAGES_PROJECT_NAME as an env variable to your Pages project name'
    )
  }
  
  // 提取到循环外部：全局只获取一次当前生产环境的 ID
  const productionDeploymentId = await getProductionDeploymentId()
  if (productionDeploymentId !== null) {
    console.log(
      `Found live production deployment to exclude from deletion: ${productionDeploymentId}`
    )
  }

  let deploymentIds = await listNextDeployments()
  while(deploymentIds.length > 1) {
    // 将生产环境 ID 传递给删除逻辑
    await deleteBatch(deploymentIds, productionDeploymentId)
    deploymentIds = await listNextDeployments()
  }
  
  console.log('Cleanup process finished.')
}

main()
