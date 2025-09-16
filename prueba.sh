JOB_ID=$(sbatch --parsible catalogues/catalog_executor.job | awk '{print $4}')
echo "Job ID: $JOB_ID"
echo "JOB_ID=$JOB_ID" >> $GITHUB_ENV
echo "Esperando a que termine el job $JOB_ID..."
while squeue -j $JOB_ID | grep -q $JOB_ID; do
  sleep 10
done
echo "Job $JOB_ID terminado."
git config --global user.name "github-actions[bot]"
git config --global user.email "github-actions[bot]@users.noreply.github.com"
git add -A
git commit -m "Auto-update catalogues [skip ci]" || echo "No changes to commit"
git push
