const fs = require('fs');
const csv = require('csv-parser');
const { write: csvWrite } = require('fast-csv');

let data = {};

// 读取并合并 CSV 文件的内容
function readCsv(filePath) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        const screenName = row.screen_name;
        if (!data[screenName]) {
          data[screenName] = row;
        } else {
          // 比较并保留 followers_count 的最大值
          if (parseInt(row.followers_count, 10) > parseInt(data[screenName].followers_count, 10)) {
            data[screenName].followers_count = row.followers_count;
          }
          // 比较并保留最早的 created_at 日期
          if (new Date(row.created_at) < new Date(data[screenName].created_at)) {
            data[screenName].created_at = row.created_at;
          }
        }
      })
      .on('end', () => {
        resolve();
      })
      .on('error', reject);
  });
}

async function mergeCsvFiles(filePaths, outputFilePath) {
  for (const filePath of filePaths) {
    await readCsv(filePath);
  }

  // 将合并后的数据写入到新的 CSV 文件
  csvWrite(Object.values(data), { headers: true })
    .pipe(fs.createWriteStream(outputFilePath));
}

// 使用示例
const csvFiles = ['csv_file1.csv', 'csv_file2.csv'];
const outputCsv = 'merged_data.csv';

mergeCsvFiles(csvFiles, outputCsv)
  .then(() => console.log('CSV files have been merged and written to', outputCsv))
  .catch(console.error);
