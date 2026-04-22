import axios from 'axios';
import fs from 'fs';

const token = fs.readFileSync('github_token.txt', 'utf8').trim();

async function createRepo() {
  try {
    const response = await axios.post(
      'https://api.github.com/user/repos',
      {
        name: 'zagros-osint',
        description: 'Zagros OSINT Tool - Discord Data Search',
        private: false,
        auto_init: true
      },
      {
        headers: {
          'Authorization': `token ${token}`,
          'Accept': 'application/vnd.github.v3+json'
        }
      }
    );
    console.log('✅ Repo oluşturuldu:', response.data.html_url);
    return response.data;
  } catch (error) {
    console.error('❌ Hata:', error.response?.data?.message || error.message);
    process.exit(1);
  }
}

createRepo();
