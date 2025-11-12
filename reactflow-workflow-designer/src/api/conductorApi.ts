import axios from 'axios';
// Minimal client: register workflow metadata to Conductor.

// Minimal axios instance
const axiosInstance = axios.create({ timeout: 10000, validateStatus: (s) => s < 500 });

function buildUrl(conductorUrl: string, path: string): string {
  const base = conductorUrl && conductorUrl.trim() !== '' ? conductorUrl.replace(/\/+$/, '') : '';
  return `${base}${path}`;
}

interface ConductorWorkflow {
  name: string;
  version: number;
  tasks: any[];
  [key: string]: any;
}

// Minimal: POST workflow metadata only
export async function deployConductorWorkflow(
  conductorUrl: string,
  workflow: ConductorWorkflow
): Promise<void> {
  const url = buildUrl(conductorUrl, '/api/metadata/workflow');
  console.debug('POST', url, workflow);
  const response = await axiosInstance.post(url, workflow, {
    headers: { 'Content-Type': 'application/json' },
  });

  if (response.status === 200 || response.status === 204) return;
  if (response.status === 409) {
    throw new Error('Workflow already exists. Update version or delete existing.');
  }
  if (response.status >= 400) {
    throw new Error(response.data?.message || `Failed with ${response.status}`);
  }
}

