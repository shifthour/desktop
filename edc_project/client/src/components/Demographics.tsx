import React, { useState } from 'react';
import {
  Box,
  Paper,
  Typography,
  TextField,
  Select,
  MenuItem,
  FormControl,
  Grid,
  Button
} from '@mui/material';

interface DemographicsData {
  gender: string;
  dateOfBirth: string;
  ageYears: string;
  heightCm: string;
  ethnicity: string;
  race: string;
  raceOther: string;
  reproductiveStatus: string;
  contraceptionMethod: string;
  contraceptionOther: string;
  reasonNo: string;
  provideFromWhen: string;
  isSubjectPregnant: string;
  isGivingBreastFeeding: string;
  isCurrentSmoker: string;
  isAlcoholicOrSubstanceUser: string;
}

const Demographics: React.FC = () => {
  const [formData, setFormData] = useState<DemographicsData>({
    gender: '',
    dateOfBirth: '',
    ageYears: '',
    heightCm: '',
    ethnicity: '',
    race: '',
    raceOther: '',
    reproductiveStatus: '',
    contraceptionMethod: '',
    contraceptionOther: '',
    reasonNo: '',
    provideFromWhen: '',
    isSubjectPregnant: '',
    isGivingBreastFeeding: '',
    isCurrentSmoker: '',
    isAlcoholicOrSubstanceUser: ''
  });

  const handleChange = (field: keyof DemographicsData, value: string) => {
    setFormData(prev => ({
      ...prev,
      [field]: value
    }));
  };

  return (
    <Box sx={{ p: 3, maxWidth: 800, margin: '0 auto' }}>
      <Paper sx={{ p: 3 }}>
        <Typography variant="h5" sx={{ mb: 3, fontWeight: 'bold', backgroundColor: '#e0e0e0', p: 1 }}>
          DEMOGRAPHICS
        </Typography>

        <Grid container spacing={3}>
          {/* Gender */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Gender :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.gender}
                    onChange={(e) => handleChange('gender', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="male">Male</MenuItem>
                    <MenuItem value="female">Female</MenuItem>
                    <MenuItem value="other">Other</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* Date of Birth */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Date of Birth :</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField
                  type="date"
                  size="small"
                  value={formData.dateOfBirth}
                  onChange={(e) => handleChange('dateOfBirth', e.target.value)}
                  InputLabelProps={{ shrink: true }}
                  sx={{ width: 200 }}
                />
              </Grid>
            </Grid>
          </Grid>

          {/* Age */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Age (Years) :</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField
                  size="small"
                  value={formData.ageYears}
                  onChange={(e) => handleChange('ageYears', e.target.value)}
                  sx={{ width: 200 }}
                />
              </Grid>
            </Grid>
          </Grid>

          {/* Height */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Height (cm) :</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField
                  size="small"
                  value={formData.heightCm}
                  onChange={(e) => handleChange('heightCm', e.target.value)}
                  sx={{ width: 200 }}
                />
              </Grid>
            </Grid>
          </Grid>

          {/* Ethnicity */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Ethnicity :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.ethnicity}
                    onChange={(e) => handleChange('ethnicity', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="hispanic">Hispanic or Latino</MenuItem>
                    <MenuItem value="not-hispanic">Not Hispanic or Latino</MenuItem>
                    <MenuItem value="unknown">Unknown</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* Race */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Race :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.race}
                    onChange={(e) => handleChange('race', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="american-indian">American Indian or Alaska Native</MenuItem>
                    <MenuItem value="asian">Asian</MenuItem>
                    <MenuItem value="black">Black or African American</MenuItem>
                    <MenuItem value="pacific-islander">Native Hawaiian or Other Pacific Islander</MenuItem>
                    <MenuItem value="white">White</MenuItem>
                    <MenuItem value="other">Other</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* If Other, Please Specify */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>If Other, Please Specify :</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField
                  size="small"
                  value={formData.raceOther}
                  onChange={(e) => handleChange('raceOther', e.target.value)}
                  disabled={formData.race !== 'other'}
                  sx={{ width: 300 }}
                />
              </Grid>
            </Grid>
          </Grid>

          {/* Reproductive Status */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Reproductive status :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.reproductiveStatus}
                    onChange={(e) => handleChange('reproductiveStatus', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="pre-menopausal">Pre-menopausal</MenuItem>
                    <MenuItem value="post-menopausal">Post-menopausal</MenuItem>
                    <MenuItem value="peri-menopausal">Peri-menopausal</MenuItem>
                    <MenuItem value="not-applicable">Not Applicable</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* If Yes, Specify contraception method */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>If Yes, Specify the method of contraception :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.contraceptionMethod}
                    onChange={(e) => handleChange('contraceptionMethod', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="oral">Oral Contraceptives</MenuItem>
                    <MenuItem value="iud">IUD</MenuItem>
                    <MenuItem value="barrier">Barrier Method</MenuItem>
                    <MenuItem value="implant">Implant</MenuItem>
                    <MenuItem value="other">Other</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* If Other, Please Specify */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>If Other, Please Specify :</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField
                  size="small"
                  value={formData.contraceptionOther}
                  onChange={(e) => handleChange('contraceptionOther', e.target.value)}
                  disabled={formData.contraceptionMethod !== 'other'}
                  sx={{ width: 300 }}
                />
              </Grid>
            </Grid>
          </Grid>

          {/* If No, select the reason */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>If No, select the reason :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.reasonNo}
                    onChange={(e) => handleChange('reasonNo', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="abstinence">Abstinence</MenuItem>
                    <MenuItem value="vasectomy">Partner Vasectomy</MenuItem>
                    <MenuItem value="tubal">Tubal Ligation</MenuItem>
                    <MenuItem value="menopause">Menopause</MenuItem>
                    <MenuItem value="other">Other</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* Provide from when */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Provide from when :</Typography>
              </Grid>
              <Grid item xs={8}>
                <TextField
                  size="small"
                  value={formData.provideFromWhen}
                  onChange={(e) => handleChange('provideFromWhen', e.target.value)}
                  sx={{ width: 300 }}
                />
              </Grid>
            </Grid>
          </Grid>

          {/* Is the Subject Pregnant? */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Is the Subject Pregnant? :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.isSubjectPregnant}
                    onChange={(e) => handleChange('isSubjectPregnant', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="yes">Yes</MenuItem>
                    <MenuItem value="no">No</MenuItem>
                    <MenuItem value="not-applicable">Not Applicable</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* Is the Subject giving Breast feeding? */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Is the Subject giving Breast feeding? :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.isGivingBreastFeeding}
                    onChange={(e) => handleChange('isGivingBreastFeeding', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="yes">Yes</MenuItem>
                    <MenuItem value="no">No</MenuItem>
                    <MenuItem value="not-applicable">Not Applicable</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* Is the Subject Current Smoker? */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Is the Subject Current Smoker? :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.isCurrentSmoker}
                    onChange={(e) => handleChange('isCurrentSmoker', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="yes">Yes</MenuItem>
                    <MenuItem value="no">No</MenuItem>
                    <MenuItem value="former">Former Smoker</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>

          {/* Is the Subject alcoholic / drug or any substance used? */}
          <Grid item xs={12}>
            <Grid container alignItems="center">
              <Grid item xs={4}>
                <Typography>Is the Subject alcoholic / drug or any substance used? :</Typography>
              </Grid>
              <Grid item xs={8}>
                <FormControl size="small" sx={{ minWidth: 200 }}>
                  <Select
                    value={formData.isAlcoholicOrSubstanceUser}
                    onChange={(e) => handleChange('isAlcoholicOrSubstanceUser', e.target.value)}
                    displayEmpty
                  >
                    <MenuItem value="">--Select--</MenuItem>
                    <MenuItem value="yes">Yes</MenuItem>
                    <MenuItem value="no">No</MenuItem>
                    <MenuItem value="former">Former User</MenuItem>
                  </Select>
                </FormControl>
              </Grid>
            </Grid>
          </Grid>
        </Grid>

        {/* Action Buttons */}
        <Box sx={{ mt: 4, display: 'flex', gap: 2, justifyContent: 'flex-end' }}>
          <Button variant="outlined">Save Draft</Button>
          <Button variant="contained" color="primary">Submit</Button>
          <Button variant="contained" color="success">Sign & Lock</Button>
        </Box>
      </Paper>
    </Box>
  );
};

export default Demographics;