Param(
    [string]$VenvDir = ".venv"
)

Write-Host "Creating Python virtual environment in '$VenvDir'..."
python -m venv $VenvDir

Write-Host "Upgrading pip and installing dependencies from requirements.txt..."
Start-Process -FilePath "$VenvDir\Scripts\python.exe" -ArgumentList '-m','pip','install','--upgrade','pip' -NoNewWindow -Wait
Start-Process -FilePath "$VenvDir\Scripts\python.exe" -ArgumentList '-m','pip','install','-r','requirements.txt' -NoNewWindow -Wait

Write-Host "Done. To activate the venv run:`n  .\$VenvDir\Scripts\Activate.ps1`"
Write-Host "To run the FastAPI app with uvicorn (example):`n  .\$VenvDir\Scripts\python.exe -m uvicorn main:app --reload`"
